import { ReplicationService } from "src/replication/replication.service";
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
 * A replica reported:
 *
 *   replica encountered an exception: no such file or directory, stat '/data/xxx@gmail.com'
 *
 * `listRecusively` stat'd every non-directory entry and let the error escape, so ONE entry that
 * readdir listed but stat could not resolve aborted the whole walk — and with it the entire
 * replication pass. Nothing replicated, and the log named only the missing path.
 *
 * readdir can legitimately report something stat cannot resolve: a broken symlink, a Windows
 * junction or reparse point that does not translate into a Linux container, an entry removed
 * between listing and stat, or a bind-mount filesystem returning DT_UNKNOWN so entries are
 * misclassified. None of those should stop a replica syncing everything else.
 */
describe("ReplicationService.listCloudDir — unreadable entries", () => {
   const CLOUD_DIR = "/data";

   type Entry = { name: string; kind: "file" | "dir" | "link" | "unstattable" | "unknown-type-dir" };

   /** Filesystem mock where each entry's behaviour under lstat/stat is chosen per test. */
   function makeFs(tree: Record<string, Entry[]>) {
      const dirent = (e: Entry) => ({
         name: e.name,
         isDirectory: () => e.kind === "dir",
         isSymbolicLink: () => e.kind === "link",
         isFile: () => e.kind === "file",
      });
      const find = (p: string): Entry | undefined => {
         const parent = p.slice(0, p.lastIndexOf("/")) || "/";
         const name = p.slice(p.lastIndexOf("/") + 1);
         return (tree[parent] ?? []).find((e) => e.name === name);
      };
      const statish = (p: string) => {
         const e = find(p);
         if (!e || e.kind === "unstattable") {
            throw Object.assign(new Error(`ENOENT: no such file or directory, stat '${p}'`), { code: "ENOENT" });
         }
         return {
            isDirectory: () => e.kind === "dir" || e.kind === "unknown-type-dir",
            isSymbolicLink: () => e.kind === "link",
            isFile: () => e.kind === "file",
            size: 100,
         };
      };
      return {
         readdirSync: jest.fn((p: string) => {
            if (!(p in tree)) throw new Error(`EACCES: permission denied, scandir '${p}'`);
            return (tree[p] ?? []).map(dirent);
         }),
         lstatSync: jest.fn(statish),
         statSync: jest.fn(statish),
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

   it("skips symlinks rather than replicating a link's target under the link's name", async () => {
      const { service } = build({
         [CLOUD_DIR]: [
            { name: "elsewhere", kind: "link" },
            { name: "real.txt", kind: "file" },
         ],
      });

      const files = await service.listCloudDir();

      expect(files.map((f) => f.path)).toEqual(["real.txt"]);
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
