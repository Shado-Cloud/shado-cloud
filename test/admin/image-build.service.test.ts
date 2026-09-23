import { ImageBuildService } from "src/admin/image-build.service";

/**
 * cloneSource is the step that decides WHICH COMMIT ends up inside the replica image, so these
 * tests pin the git invocations rather than mocking them away.
 *
 * The bug they exist to prevent: `git clone --recurse-submodules` checks out the commit the
 * superproject records in its gitlink, at a detached HEAD. Nothing in this project advances those
 * gitlinks — each service deploys itself with a `git pull` inside its own submodule directory, so
 * the pointer only moves when someone commits a bump by hand. Building from the gitlink therefore
 * shipped images built from a stale commit; the build succeeded, the smoke test passed, and the
 * only symptom was a fix that appeared to have no effect once the replica was running it.
 */
describe("ImageBuildService.cloneSource", () => {
   let service: ImageBuildService;
   /** Every git invocation, as [args, cwd]. */
   let calls: { args: string[]; cwd: string }[];

   const gitArgs = (): string[][] => calls.map(c => c.args);
   const findCall = (verb: string): string[] | undefined => gitArgs().find(a => a.includes(verb));

   beforeEach(() => {
      calls = [];
      service = new ImageBuildService({} as any);

      // Stub the process runner, not git: these tests are about the arguments passed.
      jest.spyOn(service as any, "run").mockImplementation(async (...a: unknown[]) => {
         const [cmd, args, cwd] = a as [string, string[], string];
         calls.push({ args, cwd });
         if (cmd !== "git") return "";
         // `rev-parse HEAD:<path>` reads the recorded gitlink; `rev-parse HEAD` inside the
         // submodule reads what was actually checked out. Returning different values models a
         // superproject whose pointer is behind its submodule's branch — the normal state here.
         if (args.includes("HEAD:shado-cloud")) return "64f9fe59c040722be929b920a51657b37a19d644\n";
         if (args.includes("rev-parse")) return "6382fa3edaae38703b16a96e37f2021b5fac2f21\n";
         return "";
      });
      jest.spyOn(require("fs"), "mkdtempSync").mockReturnValue("/tmp/shado-build-test");
      jest.spyOn(require("fs"), "rmSync").mockImplementation(() => undefined);
   });

   afterEach(() => jest.restoreAllMocks());

   it("does NOT clone submodules at the superproject's recorded gitlink", async () => {
      await service.cloneSource("git@github.com:Shado-Cloud/Shado-Cloud-Services.git", "main", () => {}, ["shado-cloud"]);

      const clone = findCall("clone");
      expect(clone).toBeDefined();
      // Either flag would pin the gitlink and reintroduce the stale-image bug.
      expect(clone).not.toContain("--recurse-submodules");
      expect(clone).not.toContain("--shallow-submodules");
   });

   it("checks the submodule out at its tracked branch tip via --remote", async () => {
      await service.cloneSource("git@github.com:Shado-Cloud/Shado-Cloud-Services.git", "main", () => {}, ["shado-cloud"]);

      const update = findCall("submodule");
      expect(update).toBeDefined();
      // --remote is the entire fix: without it, update checks out the recorded gitlink.
      expect(update).toContain("--remote");
      expect(update).toContain("--init");
      expect(update).toEqual(expect.arrayContaining(["--", "shado-cloud"]));
   });

   it("runs the submodule checkout inside the clone, not the temp root", async () => {
      await service.cloneSource("git@github.com:Shado-Cloud/Shado-Cloud-Services.git", "main", () => {}, ["shado-cloud"]);

      const update = calls.find(c => c.args.includes("submodule"));
      expect(update?.cwd).toBe("/tmp/shado-build-test");
   });

   it("rewrites HTTPS to SSH on the submodule fetch, not just the superproject clone", async () => {
      await service.cloneSource("git@github.com:Shado-Cloud/Shado-Cloud-Services.git", "main", () => {}, ["shado-cloud"]);

      // .gitmodules records HTTPS URLs for all nine submodules, so without the rewrite here the
      // submodule fetch asks for a username and fails under a process manager with no TTY.
      const update = findCall("submodule");
      expect(update).toEqual(
         expect.arrayContaining(["-c", "url.git@github.com:.insteadOf=https://github.com/"]),
      );
   });

   it("reports the stale gitlink alongside the commit actually built", async () => {
      const log: string[] = [];
      await service.cloneSource("git@github.com:Shado-Cloud/Shado-Cloud-Services.git", "main", c => log.push(c), [
         "shado-cloud",
      ]);

      // A silent build was how this went unnoticed, so the divergence has to reach the deploy log.
      const out = log.join("");
      expect(out).toContain("6382fa3");
      expect(out).toContain("64f9fe5");
      expect(out).toMatch(/stale/i);
   });

   it("initialises every submodule when no paths are given", async () => {
      await service.cloneSource("git@github.com:Shado-Cloud/Shado-Cloud-Services.git", "main", () => {}, []);

      const update = findCall("submodule");
      expect(update).toContain("--remote");
      // No pathspec — a bare `--` would make git treat it as an empty path list.
      expect(update).not.toContain("--");
   });

   it("removes the clone when the submodule checkout fails", async () => {
      const rmSync = jest.spyOn(require("fs"), "rmSync").mockImplementation(() => undefined);
      (service as any).run = jest.fn(async (_cmd: string, args: string[]) => {
         if (args.includes("submodule")) throw new Error("fatal: could not read Username");
         return "abc1234\n";
      });

      await expect(
         service.cloneSource("git@github.com:Shado-Cloud/Shado-Cloud-Services.git", "main", () => {}, ["shado-cloud"]),
      ).rejects.toThrow(/Could not clone/);

      // Otherwise a partial clone is left behind in the temp dir on every failed deploy.
      expect(rmSync).toHaveBeenCalledWith("/tmp/shado-build-test", { recursive: true, force: true });
   });
});

/**
 * The only thing that makes a frontend buildable from a clean clone.
 *
 * These are `adapter-static` + Vite, so `VITE_*` values are compiled INTO the bundle at build time,
 * and the `.env` files holding them are gitignored — a fresh clone contains none of them and would
 * produce a bundle whose API URLs are undefined. The values therefore come from the primary at
 * build time, and must be gone again afterwards.
 *
 * Run against a real temp directory rather than a mocked fs: the whole behaviour is file copying,
 * and mocking it would only assert that the mock was called.
 */
describe("ImageBuildService.stageEnvFile", () => {
   const fs = require("fs") as typeof import("fs");
   const os = require("os") as typeof import("os");
   const nodePath = require("path") as typeof import("path");

   let service: ImageBuildService;
   let dir: string;
   let source: string;
   let context: string;

   beforeEach(() => {
      service = new ImageBuildService({} as any);
      dir = fs.mkdtempSync(nodePath.join(os.tmpdir(), "stage-env-test-"));
      source = nodePath.join(dir, "primary.env");
      context = nodePath.join(dir, "context");
      fs.mkdirSync(context);
      fs.writeFileSync(source, "VITE_API_URL=https://cloud.example.com\n");
   });

   afterEach(() => fs.rmSync(dir, { recursive: true, force: true }));

   it("copies the env file into the context as .env", () => {
      service.stageEnvFile(source, context, () => {});

      // `.env` specifically, because that is where Vite looks.
      expect(fs.readFileSync(nodePath.join(context, ".env"), "utf-8")).toContain("VITE_API_URL=https://cloud.example.com");
   });

   it("removes it again via the disposer", () => {
      const dispose = service.stageEnvFile(source, context, () => {});
      dispose();

      // Must not survive the build: a later build in the same directory would otherwise inherit
      // another service's configuration.
      expect(fs.existsSync(nodePath.join(context, ".env"))).toBe(false);
   });

   it("restores a pre-existing .env rather than destroying it", () => {
      const target = nodePath.join(context, ".env");
      fs.writeFileSync(target, "PRE=existing\n");

      const dispose = service.stageEnvFile(source, context, () => {});
      expect(fs.readFileSync(target, "utf-8")).toContain("VITE_API_URL");
      dispose();

      expect(fs.readFileSync(target, "utf-8")).toBe("PRE=existing\n");
   });

   /*
    * Refused, not skipped. A frontend built with no env produces a bundle that loads and then fails
    * every request — far harder to diagnose than a build that stops here and names the path.
    */
   it("throws when the source does not exist on this host", () => {
      expect(() => service.stageEnvFile(nodePath.join(dir, "missing.env"), context, () => {})).toThrow(
         /does not exist on this host/,
      );
      expect(fs.existsSync(nodePath.join(context, ".env"))).toBe(false);
   });

   it("reports what it staged, so the deploy log records where the values came from", () => {
      const lines: string[] = [];
      service.stageEnvFile(source, context, (c) => lines.push(c));

      expect(lines.join("")).toContain(source);
   });
});
