import { Logger } from "@nestjs/common";
import { AdminService } from "src/admin/admin.service";
import { DirectoriesService } from "src/directories/directories.service";

/**
 * Cross-cutting contract tests for cold storage.
 *
 * TieredStorageService moves a stale file to a cold drive and leaves a SYMLINK in its place, on the
 * explicit promise that nothing else in the app can tell the difference — "reads, stats and
 * directory listings just work", because the kernel resolves the link.
 *
 * Every test in this file pins one consumer against that promise. They exist because the promise
 * was only ever documented, never asserted: the replication walk started classifying symlinks
 * separately and silently dropped every demoted file from the backup, and because the replica reads
 * "absent from the master's list" as "deleted on the master", it then unlinked its own good copies.
 * The build was green throughout. Nothing anywhere said "a cold file is still a file".
 *
 * The two rules a consumer must follow:
 *
 *   1. Classify with `!isDirectory()`, never with `isFile()` or `isSymbolicLink()`. A Dirent
 *      reflects lstat, so a cold file reports isFile() === false and would be dropped.
 *   2. Size and stat with `statSync`, never `lstatSync`. lstat describes the link (its size is the
 *      length of the target path — around 40 bytes), not the file.
 */

/** A Dirent as readdir reports it for a cold-tiered file: a symlink, and NOT a file. */
const coldDirent = (name: string) =>
   ({ name, isDirectory: () => false, isFile: () => false, isSymbolicLink: () => true }) as any;

const hotDirent = (name: string) =>
   ({ name, isDirectory: () => false, isFile: () => true, isSymbolicLink: () => false }) as any;

const dirDirent = (name: string) =>
   ({ name, isDirectory: () => true, isFile: () => false, isSymbolicLink: () => false }) as any;

const COLD_SIZE = 5_000_000;
/** What lstat would report for the link itself — the trap these tests guard. */
const LINK_SIZE = 42;

describe("cold storage: AdminService.getDirSize", () => {
   /** getDirSize is private; disk-usage reporting is its only caller. */
   const sizeOf = (service: AdminService, dir: string): Promise<number> =>
      (service as unknown as { getDirSize: (d: string) => Promise<number> }).getDirSize(dir);

   function build(tree: Record<string, unknown[]>, sizes: Record<string, number>) {
      const fs = {
         readdirSync: jest.fn((p: string) => tree[p] ?? []),
         statSync: jest.fn((p: string) => {
            if (!(p in sizes)) {
               throw Object.assign(new Error(`ENOENT: no such file or directory, stat '${p}'`), { code: "ENOENT" });
            }
            return { size: sizes[p] };
         }),
      };
      const service = Object.create(AdminService.prototype) as AdminService;
      Object.assign(service, { fs, logger: new Logger("test") });
      return { service, fs };
   }

   it("counts a cold file's real bytes, not the size of the symlink", async () => {
      const { service } = build(
         { "/cloud": [hotDirent("hot.txt"), coldDirent("cold.mkv")] },
         { "/cloud/hot.txt": 1_000, "/cloud/cold.mkv": COLD_SIZE },
      );

      // Had this used lstat, a 5 MB cold file would report ~42 bytes and the quota/usage figures
      // would collapse as files aged into cold storage.
      expect(await sizeOf(service, "/cloud")).toBe(1_000 + COLD_SIZE);
   });

   it("does not abort the whole total when a cold drive is unmounted", async () => {
      const { service } = build(
         { "/cloud": [hotDirent("hot.txt"), coldDirent("unreachable.mkv")] },
         { "/cloud/hot.txt": 1_000 }, // unreachable.mkv deliberately absent -> stat throws
      );

      // An unmounted drive makes every file on it unresolvable at once; a usage total is not worth
      // failing the admin dashboard over.
      expect(await sizeOf(service, "/cloud")).toBe(1_000);
   });

   it("recurses and counts cold files in subdirectories", async () => {
      const { service } = build(
         {
            "/cloud": [dirDirent("user")],
            "/cloud/user": [coldDirent("a.mkv")],
         },
         { "/cloud/user/a.mkv": COLD_SIZE },
      );

      expect(await sizeOf(service, "/cloud")).toBe(COLD_SIZE);
   });
});

describe("cold storage: DirectoriesService.list", () => {
   function build(entries: unknown[]) {
      const fs = {
         readdirSync: jest.fn(() => entries),
         statSync: jest.fn(() => ({ size: COLD_SIZE, mtime: new Date("2026-01-01T00:00:00Z") })),
         lstatSync: jest.fn(() => ({ size: LINK_SIZE, mtime: new Date("2026-01-01T00:00:00Z"), isDirectory: () => false })),
      };
      const fileService = {
         absolutePath: jest.fn(async () => "/cloud/user"),
         isOwner: jest.fn(async () => true),
         getUserRootPath: jest.fn(async () => "/cloud/user"),
         // The real implementation sizes with statSync; record what it is asked about.
         info: jest.fn(async (_uid: number, rel: string) => ({
            name: rel.split("/").pop(),
            path: rel,
            is_dir: false,
            size: COLD_SIZE,
         })),
      };
      const service = Object.create(DirectoriesService.prototype) as DirectoriesService;
      Object.assign(service, { fs, fileService, logger: new Logger("test") });
      return { service, fs, fileService };
   }

   it("lists a cold file as an ordinary file rather than omitting it", async () => {
      const { service, fileService } = build([hotDirent("hot.txt"), coldDirent("cold.mkv")]);

      const result: any = await service.list(1, "", false, false);

      // The `else` branch is reached via !isDirectory(), so a symlink goes down the file path.
      // Classifying on isFile() instead would have dropped cold.mkv from the user's own listing.
      expect(fileService.info).toHaveBeenCalledWith(1, "cold.mkv", false, false);
      expect(result.paginatedItems.map((r: any) => r.name)).toContain("cold.mkv");
   });

   it("keeps listing the rest of a directory when one cold file's drive is unmounted", async () => {
      const { service, fileService } = build([coldDirent("unreachable.mkv"), hotDirent("hot.txt")]);
      fileService.info.mockImplementation(async (_uid: number, rel: string) => {
         if (rel === "unreachable.mkv") {
            throw Object.assign(new Error("ENOENT: no such file or directory, stat"), { code: "ENOENT" });
         }
         return { name: rel, path: rel, is_dir: false, size: 1_000 };
      });

      const result: any = await service.list(1, "", false, false);

      // An unmounted cold drive makes every file on it fail at once; the user must still see the
      // files that are present rather than an empty or errored directory.
      expect(result.paginatedItems.map((r: any) => r.name)).toEqual(["hot.txt"]);
   });

   it("does not mistake a cold file for a directory", async () => {
      const { service, fileService } = build([coldDirent("cold.mkv")]);

      await service.list(1, "", false, false);

      // If a cold file were treated as a directory, the UI would render it as a folder that cannot
      // be opened. getUserRootPath is only called on the directory branch.
      expect(fileService.getUserRootPath).not.toHaveBeenCalled();
      expect(fileService.info).toHaveBeenCalled();
   });
});


describe("cold storage: FilesService.getBackups", () => {
   /**
    * `present` is tri-state on purpose: true / false / null-for-unknown. A cold-tiered file whose
    * drive is unmounted belongs in the third state, not the second — the bytes exist, the drive is
    * simply not attached. Reporting `false` tells the user their only primary copy is GONE, which is
    * exactly the wrong thing to say about a file that was demoted precisely because it was safe and
    * untouched.
    */
   function build(opts: { reachable: boolean; entryPresent: boolean }) {
      const fs = {
         // existsSync follows the symlink, so it is false for an unmounted cold drive.
         existsSync: jest.fn((p: string) => (String(p).includes("/mirror") ? false : opts.reachable)),
         lstatSync: jest.fn(() => {
            if (!opts.entryPresent) throw new Error("ENOENT: no such file or directory, lstat");
            return { isDirectory: () => false, isSymbolicLink: () => true, size: LINK_SIZE };
         }),
      };
      const service = Object.create(
         (require("src/files/files.service") as { FilesService: any }).FilesService.prototype,
      );
      Object.assign(service, {
         fs,
         config: { get: jest.fn((k: string) => (k === "this-service.cloud-dir" ? "/cloud" : undefined)) },
         userService: { getById: jest.fn(async () => null), isAdmin: jest.fn(async () => false) },
         absolutePath: jest.fn(async () => "/cloud/user/movie.mkv"),
         isOwner: jest.fn(async () => true),
         redis: { hgetall: jest.fn(async () => ({})) },
         logger: new Logger("test"),
      });
      return { service, fs };
   }

   const primaryOf = async (service: any) =>
      (await service.getBackups(1, "movie.mkv")).locations.find((l: any) => l.kind === "primary");

   it("reports a hot file as present", async () => {
      const { service } = build({ reachable: true, entryPresent: true });
      expect(await primaryOf(service)).toEqual(
         expect.objectContaining({ present: true, detail: "Primary copy" }),
      );
   });

   it("reports a cold file on an unmounted drive as unknown, not missing", async () => {
      const { service } = build({ reachable: false, entryPresent: true });

      const primary: any = await primaryOf(service);

      expect(primary.present).toBeNull();
      expect(primary.detail).toMatch(/cold storage/i);
   });

   it("still reports a genuinely absent file as missing", async () => {
      const { service } = build({ reachable: false, entryPresent: false });

      // The distinction must not swallow real data loss.
      expect(await primaryOf(service)).toEqual(
         expect.objectContaining({ present: false, detail: "Missing from primary storage" }),
      );
   });
});
