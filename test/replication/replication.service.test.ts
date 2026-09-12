import { Logger } from "@nestjs/common";
import { ReplicationService, LISTING_COMPLETE_HEADER } from "src/replication/replication.service";
import { ReplicationRole } from "src/config/config.validator";

/**
 * Regression test for the stale-replica alert de-duplication.
 *
 * The hourly `checkStaleReplicas` cron can end up running more than once per tick
 * (e.g. when ScheduleModule.forRoot() is registered by multiple modules in the same
 * process). Before the fix it emailed *then* removed the registry entry, so several
 * concurrent runs all saw the same stale entry and each sent an email — one stale
 * replica produced multiple emails.
 *
 * The fix claims the entry atomically with HDEL (which returns the number of fields
 * actually removed) *before* sending, so exactly one run wins and emails.
 */
describe("ReplicationService.checkStaleReplicas — alert de-duplication", () => {
   const REGISTRY_KEY = ReplicationService.REPLICAS_KEY;
   const NOW = Date.now();
   const STALE = NOW - 25 * 60 * 60 * 1000; // 25h idle (> 24h cutoff)
   const FRESH = NOW - 60 * 1000; // 1 min idle

   /** Redis mock backed by a real Map, so HDEL is a genuine atomic claim (1 once, then 0). */
   function makeRedis(entries: Record<string, unknown>) {
      const store = new Map<string, string>(
         Object.entries(entries).map(([k, v]) => [k, JSON.stringify(v)]),
      );
      return {
         store,
         hgetall: jest.fn(async () => Object.fromEntries(store)),
         hdel: jest.fn(async (_key: string, field: string) => (store.delete(field) ? 1 : 0)),
      };
   }

   function build(entries: Record<string, unknown>) {
      const redis = makeRedis(entries);
      const email = { sendEmail: jest.fn().mockResolvedValue(undefined) };
      const config = { get: jest.fn().mockReturnValue(ReplicationRole.Master) };
      const service = new ReplicationService(
         config as any,
         {} as any, // fs — unused by checkStaleReplicas
         redis as any,
         email as any,
      );
      return { service, redis, email };
   }

   const staleRecord = (overrides: Record<string, unknown> = {}) => ({
      ip: "1.2.3.4",
      deviceName: "replica-box",
      userAgent: "Service/Shado-Cloud",
      requestCount: 42,
      firstSeenAt: NOW - 30 * 24 * 60 * 60 * 1000,
      lastSeenAt: STALE,
      ...overrides,
   });

   it("emails exactly once even when the cron runs 3× concurrently for one stale replica", async () => {
      const { service, email, redis } = build({ "1.2.3.4|replica-box": staleRecord() });

      // Simulate ScheduleModule firing the same cron three times on the same tick.
      await Promise.all([
         service.checkStaleReplicas(),
         service.checkStaleReplicas(),
         service.checkStaleReplicas(),
      ]);

      expect(email.sendEmail).toHaveBeenCalledTimes(1);
      // The entry was claimed/removed from the registry.
      expect(redis.store.has("1.2.3.4|replica-box")).toBe(false);
   });

   it("does not email or remove a replica that is still within the 24h window", async () => {
      const { service, email, redis } = build({ "5.6.7.8|fresh": staleRecord({ lastSeenAt: FRESH }) });

      await service.checkStaleReplicas();

      expect(email.sendEmail).not.toHaveBeenCalled();
      expect(redis.store.has("5.6.7.8|fresh")).toBe(true);
   });

   it("emails once per distinct stale replica", async () => {
      const { service, email } = build({
         "1.1.1.1|a": staleRecord({ ip: "1.1.1.1", deviceName: "a" }),
         "2.2.2.2|b": staleRecord({ ip: "2.2.2.2", deviceName: "b" }),
      });

      await service.checkStaleReplicas();

      expect(email.sendEmail).toHaveBeenCalledTimes(2);
   });

   it("is a no-op on a replica node (only the master alerts)", async () => {
      const redis = makeRedis({ "1.2.3.4|replica-box": staleRecord() });
      const email = { sendEmail: jest.fn() };
      const config = { get: jest.fn().mockReturnValue(ReplicationRole.Replica) };
      const service = new ReplicationService(config as any, {} as any, redis as any, email as any);

      await service.checkStaleReplicas();

      expect(email.sendEmail).not.toHaveBeenCalled();
      expect(redis.hgetall).not.toHaveBeenCalled();
   });
});


/**
 * Regression tests for listing the cloud dir.
 *
 * Two bugs live here, and the second was caused by the fix for the first.
 *
 * 1. A replica reported:
 *
 *      replica encountered an exception: no such file or directory, stat '/data/xxx@gmail.com'
 *
 *    `listRecusively` stat'd every non-directory entry and let the error escape, so ONE entry that
 *    readdir listed but stat could not resolve aborted the whole walk — and with it the entire
 *    replication pass. Nothing replicated, and the log named only the missing path.
 *
 * 2. That fix skipped every symlink. But a file demoted by TieredStorageService IS a symlink,
 *    pointing at /mnt/<drive>/cloud-dir/<rel>, and the whole design of cold storage is that the
 *    rest of the app cannot tell: the kernel resolves the link, so a plain `statSync` sees an
 *    ordinary file of the real size. Skipping symlinks therefore dropped every cold file from the
 *    master's listing — and since the replica reads "absent from the master's list" as "deleted on
 *    the master", it then unlinked its own good copies. Ordinary tiering activity destroyed the
 *    backup, silently, with only a "Skipping symlink" line to show for it.
 *
 * So: follow links (cold files are files), skip only what genuinely cannot be resolved, and report
 * whether anything was skipped so a caller never mistakes an unresolvable file for a deleted one.
 */
describe("ReplicationService.listCloudDir — cold storage and unreadable entries", () => {
   const CLOUD_DIR = "/data";
   const COLD_TARGET = "/mnt/coldhdd/cloud-dir/movie.mkv";
   /** Real size of the cold blob, as `statSync` (which follows the link) reports it. */
   const COLD_SIZE = 5_000_000;
   /** What lstat reports for the link itself — the length of the target path, not the file. */
   const LINK_SIZE = COLD_TARGET.length;

   type Kind =
      | "file"
      | "dir"
      /** Demoted to cold storage: a symlink that resolves to a real file on the cold drive. */
      | "cold"
      /** A cold symlink whose drive is not mounted, so the target does not resolve. */
      | "broken-link"
      /** A symlink to a directory — never produced by tiering. */
      | "dir-link"
      /** Neither lstat nor stat can resolve it. */
      | "unstattable"
      /** A real directory that readdir reported as DT_UNKNOWN. */
      | "unknown-type-dir";

   type Entry = { name: string; kind: Kind };

   /**
    * Filesystem mock in which lstat and stat DIFFER, which is the whole point: for a cold file
    * lstat sees a small symlink and stat sees the real bytes. A mock that used one function for
    * both could not have caught this bug.
    */
   function makeFs(tree: Record<string, Entry[]>) {
      const dirent = (e: Entry) => ({
         name: e.name,
         isDirectory: () => e.kind === "dir",
         isSymbolicLink: () => e.kind === "cold" || e.kind === "broken-link" || e.kind === "dir-link",
         isFile: () => e.kind === "file",
      });
      const find = (p: string): Entry | undefined => {
         const parent = p.slice(0, p.lastIndexOf("/")) || "/";
         const name = p.slice(p.lastIndexOf("/") + 1);
         return (tree[parent] ?? []).find((e) => e.name === name);
      };
      const enoent = (p: string, syscall: string) =>
         Object.assign(new Error(`ENOENT: no such file or directory, ${syscall} '${p}'`), { code: "ENOENT" });

      // lstat: describes the entry itself, never the link target.
      const lstatSync = jest.fn((p: string) => {
         const e = find(p);
         if (!e || e.kind === "unstattable") throw enoent(p, "lstat");
         const isLink = e.kind === "cold" || e.kind === "broken-link" || e.kind === "dir-link";
         return {
            isDirectory: () => e.kind === "dir" || e.kind === "unknown-type-dir",
            isSymbolicLink: () => isLink,
            isFile: () => e.kind === "file",
            size: isLink ? LINK_SIZE : 100,
         };
      });

      // stat: follows links. This is what makes a cold file look like an ordinary file.
      const statSync = jest.fn((p: string) => {
         const e = find(p);
         if (!e || e.kind === "unstattable") throw enoent(p, "stat");
         if (e.kind === "broken-link") throw enoent(COLD_TARGET, "stat");
         return {
            isDirectory: () => e.kind === "dir" || e.kind === "unknown-type-dir" || e.kind === "dir-link",
            isSymbolicLink: () => false, // resolved
            isFile: () => e.kind === "file" || e.kind === "cold",
            size: e.kind === "cold" ? COLD_SIZE : 100,
         };
      });

      return {
         readdirSync: jest.fn((p: string) => {
            if (!(p in tree)) throw new Error(`EACCES: permission denied, scandir '${p}'`);
            return (tree[p] ?? []).map(dirent);
         }),
         lstatSync,
         statSync,
         readlinkSync: jest.fn(() => COLD_TARGET),
      };
   }

   function build(tree: Record<string, Entry[]>) {
      const fs = makeFs(tree);
      const service = new ReplicationService(
         { get: jest.fn().mockReturnValue(CLOUD_DIR) } as any,
         fs as any,
         {} as any,
         {} as any,
      );
      return { service, fs };
   }

   it("skips an entry that cannot be stat'd and still returns the rest", async () => {
      const { service } = build({
         [CLOUD_DIR]: [
            { name: "xxx@gmail.com", kind: "unstattable" },
            { name: "readable.txt", kind: "file" },
         ],
      });

      const files = await service.listCloudDir();

      expect(files.map((f) => f.path)).toEqual(["readable.txt"]);
   });

   it("does not throw when EVERY entry is unreadable", async () => {
      const { service } = build({ [CLOUD_DIR]: [{ name: "xxx@gmail.com", kind: "unstattable" }] });

      await expect(service.listCloudDir()).resolves.toEqual([]);
   });

   it("marks the listing incomplete when an entry is skipped, and complete when none are", async () => {
      const withBadEntry = build({
         [CLOUD_DIR]: [
            { name: "xxx@gmail.com", kind: "unstattable" },
            { name: "readable.txt", kind: "file" },
         ],
      });
      const allGood = build({ [CLOUD_DIR]: [{ name: "readable.txt", kind: "file" }] });

      // The distinction the delete phase depends on.
      expect((await withBadEntry.service.listCloudDirDetailed()).complete).toBe(false);
      expect((await allGood.service.listCloudDirDetailed()).complete).toBe(true);
   });

   it("marks the listing incomplete when a whole directory is unreadable", async () => {
      const { service } = build({
         [CLOUD_DIR]: [
            { name: "locked", kind: "dir" },
            { name: "ok.txt", kind: "file" },
         ],
      });

      // An unreadable directory hides an unknown number of files — by far the most dangerous case
      // to mistake for deletion.
      expect((await service.listCloudDirDetailed()).complete).toBe(false);
   });

   it("still recurses into sibling directories past an unreadable entry", async () => {
      const { service } = build({
         [CLOUD_DIR]: [
            { name: "broken", kind: "unstattable" },
            { name: "user@example.com", kind: "dir" },
         ],
         [`${CLOUD_DIR}/user@example.com`]: [{ name: "doc.txt", kind: "file" }],
      });

      const files = await service.listCloudDir();

      expect(files.map((f) => f.path)).toEqual(["user@example.com/doc.txt"]);
   });

   it("treats a directory as a directory even when readdir reports an unknown type", async () => {
      // Some bind-mount filesystems return DT_UNKNOWN, making Dirent.isDirectory() false for real
      // directories — which previously sent them down the file path and stat'd them as files.
      const { service } = build({
         [CLOUD_DIR]: [{ name: "user@example.com", kind: "unknown-type-dir" }],
         [`${CLOUD_DIR}/user@example.com`]: [{ name: "doc.txt", kind: "file" }],
      });

      const files = await service.listCloudDir();

      expect(files.map((f) => f.path)).toEqual(["user@example.com/doc.txt"]);
   });

   it("lists a cold-tiered file as an ordinary file, with the size of the cold blob", async () => {
      // This is the regression that dropped every demoted file from replication.
      const { service } = build({
         [CLOUD_DIR]: [
            { name: "movie.mkv", kind: "cold" },
            { name: "real.txt", kind: "file" },
         ],
      });

      const files = await service.listCloudDir();

      expect(files.map((f) => f.path).sort()).toEqual(["movie.mkv", "real.txt"]);
      // COLD_SIZE, not LINK_SIZE: lstat would have reported the length of the target path, so the
      // replica would have downloaded the file and then believed a 43-byte copy was complete.
      expect(files.find((f) => f.path === "movie.mkv")?.size).toBe(COLD_SIZE);
   });

   it("keeps a listing containing only cold files complete", async () => {
      const { service } = build({ [CLOUD_DIR]: [{ name: "movie.mkv", kind: "cold" }] });

      const { files, complete } = await service.listCloudDirDetailed();

      // A fully-tiered directory is entirely normal and must not read as "everything vanished".
      expect(files).toHaveLength(1);
      expect(complete).toBe(true);
   });

   it("recurses into directories and lists cold files inside them", async () => {
      const { service } = build({
         [CLOUD_DIR]: [{ name: "user@example.com", kind: "dir" }],
         [`${CLOUD_DIR}/user@example.com`]: [
            { name: "Music", kind: "dir" },
         ],
         [`${CLOUD_DIR}/user@example.com/Music`]: [{ name: "movie.mkv", kind: "cold" }],
      });

      const files = await service.listCloudDirDetailed();

      expect(files.files).toEqual([
         expect.objectContaining({ path: "user@example.com/Music/movie.mkv", size: COLD_SIZE }),
      ]);
      expect(files.complete).toBe(true);
   });

   it("skips a cold symlink whose drive is unmounted, and marks the listing incomplete", async () => {
      const { service } = build({
         [CLOUD_DIR]: [
            { name: "movie.mkv", kind: "broken-link" },
            { name: "real.txt", kind: "file" },
         ],
      });

      const { files, complete } = await service.listCloudDirDetailed();

      // Skipping is right — the bytes are genuinely unreachable — but the caller has to know the
      // listing is partial, or it will conclude the file was deleted.
      expect(files.map((f) => f.path)).toEqual(["real.txt"]);
      expect(complete).toBe(false);
   });

   it("names the unresolved link target so an unmounted cold drive is diagnosable", async () => {
      const warn = jest.spyOn(Logger.prototype, "warn").mockImplementation(() => undefined);
      const { service } = build({ [CLOUD_DIR]: [{ name: "movie.mkv", kind: "broken-link" }] });

      await service.listCloudDir();

      // "no such file or directory" on a path readdir just listed is otherwise baffling.
      const messages = warn.mock.calls.map((c) => String(c[0])).join("\n");
      expect(messages).toContain(COLD_TARGET);
      expect(messages).toMatch(/not mounted/i);
      warn.mockRestore();
   });

   it("does not follow a symlink to a directory", async () => {
      const { service } = build({
         [CLOUD_DIR]: [
            { name: "loop", kind: "dir-link" },
            { name: "real.txt", kind: "file" },
         ],
      });

      const { files, complete } = await service.listCloudDirDetailed();

      // Tiering only ever demotes files, so a directory link is not ours — following it risks a
      // cycle or a walk outside the cloud dir.
      expect(files.map((f) => f.path)).toEqual(["real.txt"]);
      expect(complete).toBe(false);
   });

   it("skips an unreadable directory instead of aborting the walk", async () => {
      const { service } = build({
         [CLOUD_DIR]: [
            { name: "locked", kind: "dir" },
            { name: "ok.txt", kind: "file" },
         ],
         // `locked` is deliberately absent from the tree, so readdir throws EACCES on it.
      });

      const files = await service.listCloudDir();

      expect(files.map((f) => f.path)).toEqual(["ok.txt"]);
   });

   it("reports sizes and cloud-dir-relative paths", async () => {
      const { service } = build({
         [CLOUD_DIR]: [{ name: "a", kind: "dir" }],
         [`${CLOUD_DIR}/a`]: [{ name: "b.txt", kind: "file" }],
      });

      const files = await service.listCloudDir();

      expect(files).toEqual([expect.objectContaining({ path: "a/b.txt", size: 100 })]);
   });
});


/**
 * The delete phase is the only destructive thing a replica does, and it infers deletion from
 * ABSENCE: anything the replica holds that the master did not list is unlinked.
 *
 * That inference is only sound if the master's listing was complete. It is not complete when the
 * master cannot resolve an entry — and the realistic way that happens is a cold-storage drive that
 * is not mounted, which makes EVERY file demoted to it unresolvable at once. Without a gate, one
 * unmounted drive on the master silently wipes every cold file from every replica on the next pass,
 * destroying the only remaining copy of exactly those files that were cold because nobody had
 * touched them in a month.
 *
 * So the master states whether its listing was complete, and the replica refuses to delete on an
 * incomplete one. Re-downloading is always safe; deleting is not.
 */
describe("ReplicationService.replicate — deletions are gated on a complete master listing", () => {
   const CLOUD_DIR = "/data";
   const MASTER = "master.internal";

   const CONFIG: Record<string, unknown> = {
      "this-service.replication.role": ReplicationRole.Replica,
      "this-service.replication.master-or-replica-ip": MASTER,
      "this-service.replication.mirror-dirs": [],
      "this-service.replication.ignore-patterns": [],
      "this-service.cloud-dir": CLOUD_DIR,
      "cross-service.secret": "test-secret",
      "this-service.password-vault-salt": "test-salt",
   };

   /**
    * @param replicaHas  files present on the replica
    * @param masterLists files the master reports
    * @param completeHeader value of the completeness header, or null to omit it entirely
    */
   function build(replicaHas: string[], masterLists: string[], completeHeader: "0" | "1" | null) {
      const fs = {
         readdirSync: jest.fn((p: string) =>
            p === CLOUD_DIR
               ? replicaHas.map((name) => ({
                  name,
                  isDirectory: () => false,
                  isSymbolicLink: () => false,
                  isFile: () => true,
               }))
               : (() => {
                  throw new Error(`EACCES: permission denied, scandir '${p}'`);
               })(),
         ),
         lstatSync: jest.fn(() => ({ isDirectory: () => false, isSymbolicLink: () => false, isFile: () => true, size: 100 })),
         statSync: jest.fn(() => ({ isDirectory: () => false, isSymbolicLink: () => false, isFile: () => true, size: 100 })),
         readlinkSync: jest.fn(() => ""),
         existsSync: jest.fn(() => true),
         mkdirSync: jest.fn(),
         unlinkSync: jest.fn(),
         createWriteStream: jest.fn(),
      };

      const headers = new Map<string, string>();
      if (completeHeader !== null) headers.set(LISTING_COMPLETE_HEADER, completeHeader);

      const fetchMock = jest.fn(async () => ({
         ok: true,
         status: 200,
         headers: { get: (name: string) => headers.get(name) ?? null },
         json: async () => masterLists.map((name) => ({ name, path: name, size: 100 })),
         text: async () => "",
      }));
      global.fetch = fetchMock as unknown as typeof fetch;

      const service = new ReplicationService(
         { get: jest.fn((key: string) => CONFIG[key]) } as any,
         fs as any,
         {} as any,
         {} as any,
      );
      return { service, fs, fetchMock };
   }

   afterEach(() => jest.restoreAllMocks());

   it("deletes a file the master no longer has when the listing is complete", async () => {
      const { service, fs } = build(["gone.txt"], [], "1");

      await service.replicate();

      // Normal behaviour must be preserved — the gate must not disable deletion outright.
      expect(fs.unlinkSync).toHaveBeenCalledWith("/data/gone.txt");
   });

   it("deletes NOTHING when the master reports an incomplete listing", async () => {
      const { service, fs } = build(["movie.mkv"], [], "0");

      await service.replicate();

      // The master could not resolve movie.mkv (cold drive unmounted); it was never deleted there.
      expect(fs.unlinkSync).not.toHaveBeenCalled();
   });

   it("treats a master that sends no completeness header as complete", async () => {
      const { service, fs } = build(["gone.txt"], [], null);

      await service.replicate();

      // Backward compatibility: a master predating this change behaves exactly as it used to, so
      // master and replica can be upgraded in either order.
      expect(fs.unlinkSync).toHaveBeenCalledWith("/data/gone.txt");
   });

   it("still keeps files the master DID list, on an incomplete listing", async () => {
      const { service, fs } = build(["keep.txt", "movie.mkv"], ["keep.txt"], "0");

      await service.replicate();

      expect(fs.unlinkSync).not.toHaveBeenCalled();
   });

   it("does not delete when the master's listing is empty because its cloud dir was unreadable", async () => {
      // The worst case: the master's whole walk failed, so it lists nothing at all. Read as
      // "complete", that instructs every replica to erase everything it holds.
      const { service, fs } = build(["a.txt", "b.txt", "c.txt"], [], "0");

      await service.replicate();

      expect(fs.unlinkSync).not.toHaveBeenCalled();
   });
});
