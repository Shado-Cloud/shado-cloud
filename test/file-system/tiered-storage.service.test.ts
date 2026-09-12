import { TieredStorageService } from "src/file-system/tiered-storage.service";
import type { Dirent } from "src/file-system/abstract-file-system.interface";

const dirent = (name: string, isDir = false): Dirent =>
   ({ name, isDirectory: () => isDir, isFile: () => !isDir, isSymbolicLink: () => false } as any);

const symlinkDirent = (name: string): Dirent =>
   ({ name, isDirectory: () => false, isFile: () => false, isSymbolicLink: () => true } as any);

/**
 * The service talks to the filesystem through the injected NodeFileSystemService (synchronous
 * API), not through `fs.promises` — so the double below stands in for that provider. Every cold
 * operation is gated behind statfsSync() succeeding on the drive mount, which is why that has to
 * be stubbed even for tests that never touch a cold blob.
 */
const makeFsMock = () => ({
   statSync: jest.fn().mockReturnValue({ atimeMs: Date.now(), size: 0 }),
   lstatSync: jest.fn().mockReturnValue({ isSymbolicLink: () => false, isDirectory: () => false }),
   readdirSync: jest.fn().mockReturnValue([]),
   readlinkSync: jest.fn().mockReturnValue(""),
   mkdirSync: jest.fn(),
   copyFileSync: jest.fn(),
   symlinkSync: jest.fn(),
   renameSync: jest.fn(),
   rmSync: jest.fn(),
   statfsSync: jest.fn().mockReturnValue({ bavail: 1_000_000, bsize: 4096 }),
});

describe("TieredStorageService", () => {
   let config: { get: jest.Mock };
   let featureFlag: { isFeatureFlagEnabled: jest.Mock };
   let redis: { info: jest.Mock };
   let fsMock: ReturnType<typeof makeFsMock>;
   let metrics: {
      coldStorageDemotions: number;
      coldStorageBytesMoved: number;
      coldStorageDemotionErrors: number;
      coldStorageLastSweepMs: number;
      coldStorageLastSweepAt: number;
      coldStoragePromotions: number;
      coldStorageBytesPromoted: number;
      coldStoragePromotionErrors: number;
   };
   let service: TieredStorageService;

   const STALE = Date.now() - 200 * 24 * 60 * 60 * 1000; // ~6.5 months ago (atimeMs)
   const FRESH = Date.now();
   const BIG = 5 * 1024 * 1024;

   beforeEach(() => {
      config = {
         get: jest.fn().mockImplementation((key: string) => {
            if (key === "this-service.cloud-dir") return "/cloud";
            if (key === "this-service.cold-storage") return { drives: ["coldhdd"] };
            return undefined;
         }),
      };
      featureFlag = {
         isFeatureFlagEnabled: jest.fn().mockResolvedValue(true),
      };
      redis = {
         info: jest.fn().mockResolvedValue("used_memory:1048576\r\nmaxmemory:104857600\r\n"),
      };
      metrics = {
         coldStorageDemotions: 0, coldStorageBytesMoved: 0, coldStorageDemotionErrors: 0,
         coldStorageLastSweepMs: 0, coldStorageLastSweepAt: 0,
         coldStoragePromotions: 0, coldStorageBytesPromoted: 0, coldStoragePromotionErrors: 0,
      };
      fsMock = makeFsMock();
      service = new TieredStorageService(
         config as any,
         featureFlag as any,
         redis as any,
         fsMock as any,
         metrics as any,
      );
   });

   afterEach(() => jest.restoreAllMocks());

   describe("demoteStaleFiles", () => {
      it("demotes a stale, large file: copies to cold then atomically replaces it with a symlink", async () => {
         fsMock.readdirSync.mockReturnValue([dirent("big.bin")]);
         fsMock.statSync.mockReturnValue({ atimeMs: STALE, size: BIG });

         await service.demoteStaleFiles();

         const coldPath = "/mnt/coldhdd/cloud-dir/big.bin";
         expect(fsMock.copyFileSync).toHaveBeenCalledWith("/cloud/big.bin", coldPath);
         expect(fsMock.symlinkSync).toHaveBeenCalledWith(coldPath, expect.stringContaining("/cloud/big.bin.tiering-"));
         expect(fsMock.renameSync).toHaveBeenCalledWith(expect.stringContaining("/cloud/big.bin.tiering-"), "/cloud/big.bin");
         expect(metrics.coldStorageDemotions).toBe(1);
         expect(metrics.coldStorageBytesMoved).toBe(BIG);
         expect(metrics.coldStorageDemotionErrors).toBe(0);
         expect(metrics.coldStorageLastSweepAt).toBeGreaterThan(0);
      });

      it("skips a recently-accessed file", async () => {
         fsMock.readdirSync.mockReturnValue([dirent("recent.bin")]);
         fsMock.statSync.mockReturnValue({ atimeMs: FRESH, size: BIG });

         await service.demoteStaleFiles();

         expect(fsMock.copyFileSync).not.toHaveBeenCalled();
         expect(metrics.coldStorageDemotions).toBe(0);
      });

      it("skips a stale but small file", async () => {
         fsMock.readdirSync.mockReturnValue([dirent("tiny.txt")]);
         fsMock.statSync.mockReturnValue({ atimeMs: STALE, size: 100 });

         await service.demoteStaleFiles();

         expect(fsMock.copyFileSync).not.toHaveBeenCalled();
      });

      it("does nothing when the tiered_storage flag is disabled", async () => {
         featureFlag.isFeatureFlagEnabled.mockResolvedValue(false);
         fsMock.readdirSync.mockReturnValue([dirent("big.bin")]);

         await service.demoteStaleFiles();

         expect(fsMock.readdirSync).not.toHaveBeenCalled();
         expect(fsMock.copyFileSync).not.toHaveBeenCalled();
      });

      it("does nothing when no cold drive is configured", async () => {
         (config.get as jest.Mock).mockImplementation((key: string) =>
            key === "this-service.cloud-dir" ? "/cloud" : { drives: [] },
         );
         fsMock.readdirSync.mockReturnValue([dirent("big.bin")]);
         fsMock.statSync.mockReturnValue({ atimeMs: STALE, size: BIG });

         await service.demoteStaleFiles();

         expect(fsMock.copyFileSync).not.toHaveBeenCalled();
      });

      it("skips a cold drive that is not reachable", async () => {
         fsMock.statfsSync.mockImplementation(() => { throw new Error("ENOENT"); });
         fsMock.readdirSync.mockReturnValue([dirent("big.bin")]);
         fsMock.statSync.mockReturnValue({ atimeMs: STALE, size: BIG });

         await service.demoteStaleFiles();

         expect(fsMock.copyFileSync).not.toHaveBeenCalled();
         expect(metrics.coldStorageDemotions).toBe(0);
      });

      it("counts an error (without aborting) when a demotion fails", async () => {
         fsMock.readdirSync.mockReturnValue([dirent("big.bin")]);
         fsMock.statSync.mockReturnValue({ atimeMs: STALE, size: BIG });
         fsMock.copyFileSync.mockImplementation(() => { throw new Error("disk full"); });

         await service.demoteStaleFiles();

         expect(metrics.coldStorageDemotionErrors).toBe(1);
         expect(metrics.coldStorageDemotions).toBe(0);
      });
   });

   describe("promote-on-access (fire-and-forget)", () => {
      const COLD = "/mnt/coldhdd/cloud-dir/doc.txt";
      const promoteOnAccess = (p: string) => (service as any).promoteOnAccess(p) as Promise<void>;

      it("promotes a cold file when it is accessed", async () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue(COLD);
         fsMock.statSync.mockReturnValue({ size: 2048 });

         await promoteOnAccess("/cloud/doc.txt");

         expect(fsMock.copyFileSync).toHaveBeenCalledWith(COLD, expect.stringContaining("/cloud/doc.txt.promoting-"));
         expect(fsMock.renameSync).toHaveBeenCalledWith(expect.stringContaining("/cloud/doc.txt.promoting-"), "/cloud/doc.txt");
         expect(fsMock.rmSync).toHaveBeenCalledWith(COLD, { force: true });
         expect(metrics.coldStoragePromotions).toBe(1);
         expect(metrics.coldStorageBytesPromoted).toBe(2048);
      });

      it("is a no-op for a hot file (not a symlink) — without even checking the flag", async () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => false, isDirectory: () => false });

         await promoteOnAccess("/cloud/hot.txt");

         expect(fsMock.copyFileSync).not.toHaveBeenCalled();
         expect(featureFlag.isFeatureFlagEnabled).not.toHaveBeenCalled();
         expect(metrics.coldStoragePromotions).toBe(0);
      });

      it("does not promote a symlink pointing outside cold storage", async () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue("/some/other/place/x");

         await promoteOnAccess("/cloud/link.txt");

         expect(fsMock.copyFileSync).not.toHaveBeenCalled();
      });

      it("does not promote when the promotion flag is disabled", async () => {
         featureFlag.isFeatureFlagEnabled.mockResolvedValue(false);
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });

         await promoteOnAccess("/cloud/doc.txt");

         expect(fsMock.copyFileSync).not.toHaveBeenCalled();
      });

      it("counts a promotion error and never throws", async () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue(COLD);
         fsMock.statSync.mockReturnValue({ size: 10 });
         fsMock.copyFileSync.mockImplementation(() => { throw new Error("io error"); });

         await expect(promoteOnAccess("/cloud/doc.txt")).resolves.toBeUndefined();

         expect(metrics.coldStoragePromotionErrors).toBe(1);
         expect(metrics.coldStoragePromotions).toBe(0);
      });
   });

   describe("evacuateDrive", () => {
      it("migrates referenced cold files back to main and removes orphans", async () => {
         // /cloud has a cold symlink; /mnt/coldhdd/cloud-dir has a leftover orphan blob
         fsMock.readdirSync.mockImplementation((dir: any) => {
            if (String(dir) === "/cloud") return [symlinkDirent("ref.bin")] as any;
            if (String(dir) === "/mnt/coldhdd/cloud-dir") return [dirent("orphan.bin")] as any;
            return [] as any;
         });
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue("/mnt/coldhdd/cloud-dir/ref.bin");
         fsMock.statSync.mockReturnValue({ size: 100 });

         const result = await service.evacuateDrive("coldhdd");

         expect(result.migrated).toBe(1);
         expect(result.orphansRemoved).toBe(1);
         expect(result.errors).toBe(0);
         // referenced file copied back + symlink replaced
         expect(fsMock.copyFileSync).toHaveBeenCalledWith("/mnt/coldhdd/cloud-dir/ref.bin", expect.stringContaining("/cloud/ref.bin.promoting-"));
         // leftover orphan blob removed
         expect(fsMock.rmSync).toHaveBeenCalledWith("/mnt/coldhdd/cloud-dir/orphan.bin", { force: true });
         expect(metrics.coldStoragePromotions).toBe(1);
         expect(metrics.coldStorageBytesPromoted).toBe(100);
      });

      it("is a no-op for an unknown (non-configured) drive", async () => {
         const result = await service.evacuateDrive("not-a-drive");

         expect(result).toEqual({ migrated: 0, bytes: 0, errors: 0, orphansRemoved: 0 });
         expect(fsMock.readdirSync).not.toHaveBeenCalled();
      });
   });

   describe("removeColdData", () => {
      it("removes the cold blob backing a cold symlink", async () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue("/mnt/coldhdd/cloud-dir/x.png");

         await service.removeColdData("/cloud/x.png");

         expect(fsMock.rmSync).toHaveBeenCalledWith("/mnt/coldhdd/cloud-dir/x.png", { force: true });
      });

      it("does nothing for a hot (non-symlink) file", async () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => false, isDirectory: () => false });

         await service.removeColdData("/cloud/hot.png");

         expect(fsMock.rmSync).not.toHaveBeenCalled();
      });

      it("recurses into a directory and removes each cold blob", async () => {
         fsMock.lstatSync.mockImplementation((p: any) =>
            String(p) === "/cloud/d"
               ? ({ isSymbolicLink: () => false, isDirectory: () => true } as any)
               : ({ isSymbolicLink: () => true, isDirectory: () => false } as any),
         );
         fsMock.readdirSync.mockReturnValue([dirent("a.png")]);
         fsMock.readlinkSync.mockReturnValue("/mnt/coldhdd/cloud-dir/d/a.png");

         await service.removeColdData("/cloud/d");

         expect(fsMock.rmSync).toHaveBeenCalledWith("/mnt/coldhdd/cloud-dir/d/a.png", { force: true });
      });
   });

   describe("redisMemory", () => {
      it("parses used_memory and maxmemory from INFO", async () => {
         redis.info.mockResolvedValue("# Memory\r\nused_memory:2097152\r\nused_memory_human:2.00M\r\nmaxmemory:104857600\r\n");
         expect(await service.redisMemory()).toEqual({ usedMemory: 2097152, maxMemory: 104857600 });
      });

      it("reports maxMemory 0 when no limit is set", async () => {
         redis.info.mockResolvedValue("used_memory:500\r\nmaxmemory:0\r\n");
         expect(await service.redisMemory()).toEqual({ usedMemory: 500, maxMemory: 0 });
      });

      it("returns zeros when Redis is unavailable", async () => {
         redis.info.mockRejectedValue(new Error("connection refused"));
         expect(await service.redisMemory()).toEqual({ usedMemory: 0, maxMemory: 0 });
      });

      it("is included in getOverview", async () => {
         redis.info.mockResolvedValue("used_memory:1000\r\nmaxmemory:2000\r\n");
         const overview = await service.getOverview();
         expect(overview.redis).toEqual({ usedMemory: 1000, maxMemory: 2000 });
      });
   });

   /**
    * `coldStats` is what the UI uses to say "N of M files are in cold storage", and it is the one
    * place that is SUPPOSED to distinguish a cold symlink from a hot file. Everything else in the
    * app must treat them identically — which is the distinction that got inverted in the
    * replication walk and silently dropped every cold file from the backup.
    */
   describe("coldStats", () => {
      it("counts a cold symlink as a file, and as cold", () => {
         fsMock.readdirSync.mockImplementation((dir: string) =>
            dir === "/cloud" ? [dirent("hot.txt"), symlinkDirent("cold.mkv")] : [],
         );
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue("/mnt/coldhdd/cloud-dir/cold.mkv");

         // A cold file is still a file: total counts both, cold counts only the symlink.
         expect(service.coldStats("/cloud")).toEqual({ total: 2, cold: 1 });
      });

      it("does not count a symlink pointing outside cold storage as cold", () => {
         fsMock.readdirSync.mockImplementation((dir: string) =>
            dir === "/cloud" ? [symlinkDirent("elsewhere")] : [],
         );
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue("/home/user/somewhere-else");

         // Still a file, but not a tiered one — cold accounting must not over-report.
         expect(service.coldStats("/cloud")).toEqual({ total: 1, cold: 0 });
      });

      it("recurses into subdirectories", () => {
         fsMock.readdirSync.mockImplementation((dir: string) => {
            if (dir === "/cloud") return [dirent("user", true)];
            if (dir === "/cloud/user") return [dirent("a.txt"), symlinkDirent("b.mkv")];
            return [];
         });
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue("/mnt/coldhdd/cloud-dir/user/b.mkv");

         expect(service.coldStats("/cloud")).toEqual({ total: 2, cold: 1 });
      });

      it("ignores hidden and internal entries", () => {
         fsMock.readdirSync.mockImplementation((dir: string) =>
            dir === "/cloud" ? [dirent(".meta"), dirent("_system"), dirent("real.txt")] : [],
         );

         // Thumbnails and internal folders are not user files and would inflate the totals.
         expect(service.coldStats("/cloud")).toEqual({ total: 1, cold: 0 });
      });

      it("returns zeros for an unreadable directory instead of throwing", () => {
         fsMock.readdirSync.mockImplementation(() => {
            throw new Error("EACCES: permission denied");
         });

         expect(service.coldStats("/cloud")).toEqual({ total: 0, cold: 0 });
      });
   });

   describe("isColdFile", () => {
      it("is true for a symlink whose target is on a configured cold drive", () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue("/mnt/coldhdd/cloud-dir/movie.mkv");

         expect(service.isColdFile("/cloud/movie.mkv")).toBe(true);
      });

      it("is false for a hot file", () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => false, isDirectory: () => false });

         expect(service.isColdFile("/cloud/movie.mkv")).toBe(false);
      });

      it("is false for a symlink to an unconfigured drive", () => {
         fsMock.lstatSync.mockReturnValue({ isSymbolicLink: () => true, isDirectory: () => false });
         fsMock.readlinkSync.mockReturnValue("/mnt/someotherdrive/cloud-dir/movie.mkv");

         expect(service.isColdFile("/cloud/movie.mkv")).toBe(false);
      });

      it("is false — not a throw — for a path that does not exist", () => {
         fsMock.lstatSync.mockImplementation(() => {
            throw new Error("ENOENT: no such file or directory");
         });

         // Callers use this as a predicate on the request path; it must never throw.
         expect(service.isColdFile("/cloud/gone.mkv")).toBe(false);
      });
   });

   describe("getOverview — drives", () => {
      it("reports cold file count, cold bytes and disk usage per configured drive", async () => {
         fsMock.readdirSync.mockImplementation((dir: string) =>
            dir === "/mnt/coldhdd/cloud-dir" ? [dirent("a.mkv"), dirent("b.mkv")] : [],
         );
         fsMock.statSync.mockReturnValue({ size: 1_000_000, atimeMs: Date.now() });
         fsMock.statfsSync.mockReturnValue({ blocks: 1000, bavail: 400, bsize: 4096 });

         const overview = await service.getOverview();

         expect(overview.drives).toHaveLength(1);
         expect(overview.drives[0]).toEqual({
            name: "coldhdd",
            mountPoint: "/mnt/coldhdd",
            coldFileCount: 2,
            coldBytes: 2_000_000,
            total: 4_096_000,
            free: 1_638_400,
            used: 4_096_000 - 1_638_400,
         });
      });

      it("reports zero usage when the cold drive is not mounted", async () => {
         fsMock.readdirSync.mockReturnValue([]);
         fsMock.statfsSync.mockImplementation(() => {
            throw new Error("ENOENT: no such file or directory, statfs '/mnt/coldhdd/cloud-dir'");
         });

         const overview = await service.getOverview();

         // The admin page has to render while a drive is unplugged rather than 500.
         expect(overview.drives[0]).toEqual(
            expect.objectContaining({ name: "coldhdd", coldFileCount: 0, coldBytes: 0, total: 0, free: 0, used: 0 }),
         );
      });

      it("surfaces both feature flags", async () => {
         featureFlag.isFeatureFlagEnabled.mockImplementation(async (_ns: unknown, flag: string) =>
            flag === "tiered_storage_demotion",
         );

         const overview = await service.getOverview();

         expect(overview.flags).toEqual({ demotion: true, promotion: false });
      });
   });
});
