import { Test, TestingModule } from "@nestjs/testing";
import { ConfigService } from "@nestjs/config";
import { DeploymentService } from "src/admin/deployment.service";
import { AppLogger } from "src/logging";
import { EmailService } from "src/admin/email.service";
import { FeatureFlagService } from "src/admin/feature-flag.service";
import { REDIS_CACHE } from "src/util";
import { getRepositoryToken } from "@nestjs/typeorm";
import { DeploymentProject } from "src/models/admin/deploymentProject";
import { ReplicaPropagationService } from "src/admin/replica-propagation.service";
import { ImageBuildService } from "src/admin/image-build.service";
import * as childProcess from "child_process";
import { EventEmitter } from "events";

jest.mock("child_process");

function createMockProc() {
   const proc = new EventEmitter() as any;
   proc.stdout = new EventEmitter();
   proc.stderr = new EventEmitter();
   return proc;
}

function mockSpawnWithPwd(mainProc: any) {
   (childProcess.spawn as jest.Mock).mockImplementation((cmd: string) => {
      if (cmd === "pwd") {
         const pwdProc = createMockProc();
         process.nextTick(() => {
            pwdProc.stdout.emit("data", Buffer.from("/mocked/cwd"));
            pwdProc.emit("close", 0);
         });
         return pwdProc;
      }
      return mainProc;
   });
}

const backendSteps = [
   { step: "git_pull", name: "Git Pull", cmd: "git", args: ["pull"] },
   { step: "npm_install", name: "NPM Install", cmd: "npm", args: ["install"] },
   { step: "test", name: "Run Tests", cmd: "npm", args: ["test", "--", "--runInBand", "--no-colors"] },
   { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
   { step: "migrate", name: "Run Migrations", cmd: "npx", args: ["typeorm", "migration:run", "-d", "ormconfig.js"] },
   { step: "restart", name: "Restart Service", cmd: "pm2", args: ["restart", "shado-cloud-backend"], triggersRestart: true },
   { step: "verify", name: "Verify Deployment", cmd: "pm2", args: ["jlist"], runsOnModuleInit: true },
   {
      step: "propagate_replicas",
      name: "Propagate to Replicas",
      cmd: "",
      args: [],
      propagateToReplicas: true,
      buildImage: true,
      sourceRepo: "git@github.com:Shado-Cloud/Shado-Cloud-Services.git",
      sourceBranch: "main",
      contextSubdir: "shado-cloud",
      dockerfile: "../Dockerfile.shado-cloud",
      imageTarget: "runtime",
      imageTag: "shado-cloud:deploy",
      imageService: "shado-cloud",
      smokePort: 9000,
   },
];

/** Redis hash key recording which default-step additions each project has received. */
const REDIS_STEPS_VERSION_KEY = "deployment:seeded-steps-version";

// buildImage: false — these exercise dispatch alone, without the build half.
const propagateSteps = [
   { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
   { step: "propagate_replicas", name: "Propagate to Replicas", cmd: "", args: [], propagateToReplicas: true, buildImage: false },
];

const strictPropagateSteps = [
   { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
   { step: "propagate_replicas", name: "Propagate to Replicas", cmd: "", args: [], propagateToReplicas: true, buildImage: false, requireAllReplicas: true },
];

function makeProject(slug: string, steps: any[], workDir = "__CWD__"): DeploymentProject {
   const p = new DeploymentProject();
   p.id = 1;
   p.slug = slug;
   p.name = slug;
   p.workDir = workDir;
   p.pm2ProcessName = null;
   p.branch = "master";
   p.enabled = true;
   p.setSteps(steps);
   return p;
}

describe("DeploymentService", () => {
   let service: DeploymentService;
   let emailService: EmailService;
   let featureFlagService: FeatureFlagService;
   let logger: AppLogger;
   const redisStore: Record<string, string> = {};
   const queueStore: Record<string, string[]> = {};
   const redisHashes: Record<string, Record<string, string>> = {};
   let projectRepo: any;
   let replicaPropagation: any;
   let imageBuilder: any;

   beforeEach(async () => {
      Object.keys(redisStore).forEach(k => delete redisStore[k]);
      Object.keys(queueStore).forEach(k => delete queueStore[k]);
      Object.keys(redisHashes).forEach(k => delete redisHashes[k]);

      // Docker is available and every stage succeeds by default; individual tests override.
      imageBuilder = {
         isDockerAvailable: jest.fn().mockResolvedValue(true),
         cloneSource: jest.fn().mockResolvedValue({ dir: "/tmp/shado-build-xyz", dispose: jest.fn() }),
         build: jest.fn().mockResolvedValue("sha256:builtimage"),
         smokeTest: jest.fn().mockResolvedValue(undefined),
         stage: jest.fn().mockResolvedValue({
            artifact: "a".repeat(32),
            imageId: "sha256:builtimage",
            tarSha256: "a".repeat(64),
            size: 2 * 1024 * 1024 * 1024,
         }),
      };

      // No replicas connected by default: propagation is a no-op pass-through.
      replicaPropagation = {
         connectedReplicas: jest.fn().mockReturnValue([]),
         propagate: jest.fn().mockImplementation(async (opts: any, cb: any) => {
            const state = { runId: `${opts.deploymentId}_prop_1`, dispatchedAt: 1, finishedAt: 2, replicas: [] as any[] };
            cb.onLog("No replicas are connected to the replica-link — nothing to propagate to.\n");
            cb.onDispatch(state);
            return state;
         }),
         allSucceeded: jest.fn().mockReturnValue(true),
         summarize: jest.fn().mockReturnValue("No replicas online"),
      };

      projectRepo = {
         find: jest.fn().mockResolvedValue([makeProject("backend", backendSteps)]),
         findOneBy: jest.fn().mockImplementation(({ slug }: any) => {
            if (slug === "backend") return Promise.resolve(makeProject("backend", backendSteps));
            if (slug === "frontend") return Promise.resolve(makeProject("frontend", [
               { step: "git_pull", name: "Git Pull", cmd: "git", args: ["pull"] },
               { step: "npm_install", name: "NPM Install", cmd: "npm", args: ["install"] },
               { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
            ], "/tmp/frontend"));
            if (slug === "propagating") return Promise.resolve(makeProject("propagating", propagateSteps));
            if (slug === "strict-propagating") return Promise.resolve(makeProject("strict-propagating", strictPropagateSteps));
            return Promise.resolve(null);
         }),
         create: jest.fn((data: any) => data),
         save: jest.fn().mockResolvedValue({}),
         delete: jest.fn().mockResolvedValue({}),
      };

      const module: TestingModule = await Test.createTestingModule({
         providers: [
            DeploymentService,
            {
               provide: ConfigService,
               useValue: {
                  get: jest.fn((key: string) => {
                     if (key === "FRONTEND_DEPLOY_PATH") return "/tmp/frontend";
                     if (key === "FRONTEND_URL") return "http://localhost:3000";
                     return null;
                  }),
               },
            },
            {
               provide: AppLogger,
               useValue: { log: jest.fn(), warn: jest.fn(), error: jest.fn() },
            },
            {
               provide: EmailService,
               useValue: { sendEmail: jest.fn() },
            },
            {
               provide: FeatureFlagService,
               useValue: { isFeatureFlagDisabled: jest.fn().mockResolvedValue(false), isFeatureFlagEnabled: jest.fn().mockResolvedValue(true) },
            },
            {
               provide: ReplicaPropagationService,
               useValue: replicaPropagation,
            },
            {
               provide: ImageBuildService,
               useValue: imageBuilder,
            },
            {
               provide: REDIS_CACHE,
               useValue: {
                  get: jest.fn((key: string) => Promise.resolve(redisStore[key] || null)),
                  set: jest.fn((key: string, value: string) => { redisStore[key] = value; return Promise.resolve("OK"); }),
                  del: jest.fn((key: string) => { delete redisStore[key]; return Promise.resolve(1); }),
                  // The deployment queue. Needed by any test that lets a pipeline reach a
                  // terminal state, because both outcomes call processQueue().
                  rpush: jest.fn((key: string, value: string) => { (queueStore[key] ??= []).push(value); return Promise.resolve(queueStore[key].length); }),
                  lpop: jest.fn((key: string) => Promise.resolve(queueStore[key]?.shift() ?? null)),
                  lrange: jest.fn((key: string) => Promise.resolve([...(queueStore[key] ?? [])])),
                  // Records which default-step additions each project has already received.
                  hget: jest.fn((key: string, field: string) => Promise.resolve(redisHashes[key]?.[field] ?? null)),
                  hset: jest.fn((key: string, field: string, value: string) => {
                     (redisHashes[key] ??= {})[field] = value;
                     return Promise.resolve(1);
                  }),
               },
            },
            {
               provide: getRepositoryToken(DeploymentProject),
               useValue: projectRepo,
            },
         ],
      }).compile();

      service = module.get<DeploymentService>(DeploymentService);
      emailService = module.get<EmailService>(EmailService);
      featureFlagService = module.get<FeatureFlagService>(FeatureFlagService);
      // DeploymentService now uses a private `new Logger()`; spy on that instance.
      logger = (service as any).logger;
      jest.spyOn(logger, "error").mockImplementation();
      jest.spyOn(logger, "log").mockImplementation();
      jest.spyOn(logger, "warn").mockImplementation();
      jest.spyOn(logger, "debug").mockImplementation();
   });

   afterEach(() => {
      jest.clearAllMocks();
   });

   describe("isRunning", () => {
      it("should return false when no deployment", async () => {
         expect(await service.isRunning()).toBe(false);
      });
   });

   describe("getCurrentDeployment", () => {
      it("should return null when no deployment", async () => {
         expect(await service.getCurrentDeployment()).toBeNull();
      });
   });

   describe("getLastDeployment", () => {
      it("should return null when no previous deployment", async () => {
         expect(await service.getLastDeployment()).toBeNull();
      });
   });

   describe("startDeployment", () => {
      it("should throw if deployment already in progress", async () => {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         await service.startDeployment("backend", "test");

         await expect(service.startDeployment("backend", "test")).rejects.toThrow("Deployment already in progress");
      });

      it("should throw if project not found", async () => {
         await expect(service.startDeployment("nonexistent", "test")).rejects.toThrow('Project "nonexistent" not found');
      });

      it("should return a Subject for SSE streaming", async () => {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         const subject = await service.startDeployment("backend", "admin");

         expect(subject).toBeDefined();
         expect(typeof subject.subscribe).toBe("function");
      });

      it("should set deployment state correctly", async () => {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         await service.startDeployment("backend", "github-webhook");

         const deployment = await service.getCurrentDeployment();
         expect(deployment).not.toBeNull();
         expect(deployment?.project).toBe("backend");
         expect(deployment?.triggeredBy).toBe("github-webhook");
         expect(deployment?.status).toBe("running");
         expect(deployment?.currentStep).toBeDefined();
         expect(deployment?.currentStep.step).toBe("git_pull");
      });
   });

   describe("deployment flow", () => {
      it("should block deployment if feature flag is disabled", async () => {
         (featureFlagService.isFeatureFlagDisabled as jest.Mock).mockResolvedValue(true);

         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         const subject = await service.startDeployment("backend", "test");
         const events: any[] = [];
         subject.subscribe((event) => events.push(JSON.parse((event as any).data)));

         await new Promise((r) => setTimeout(r, 50));

         expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining("feature flag is disabled"));
         expect((await service.getCurrentDeployment())?.status).toBe("failed");
      });

      it("should send start email when deployment begins", async () => {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         await service.startDeployment("backend", "admin");

         await new Promise((r) => setTimeout(r, 50));

         expect(emailService.sendEmail).toHaveBeenCalledWith(
            expect.objectContaining({
               subject: "Shado Cloud - backend deployment started",
            }),
         );
      });

      it("should emit step events and complete successfully", async () => {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         const subject = await service.startDeployment("backend", "test");
         const events: any[] = [];
         subject.subscribe((event) => events.push(JSON.parse((event as any).data)));

         await new Promise((r) => setTimeout(r, 50));

         for (let i = 0; i < 6; i++) {
            mockProc.emit("close", 0);
            await new Promise((r) => setTimeout(r, 20));
         }

         const stepStarts = events.filter((e) => e.type === "step_start");
         const stepCompletes = events.filter((e) => e.type === "step_complete");
         expect(stepStarts.length).toBeGreaterThan(0);
         expect(stepCompletes.length).toBeGreaterThan(0);
      });

      it("should capture stdout output", async () => {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         const subject = await service.startDeployment("backend", "test");
         const events: any[] = [];
         subject.subscribe((event) => events.push(JSON.parse((event as any).data)));

         await new Promise((r) => setTimeout(r, 50));

         mockProc.stdout.emit("data", Buffer.from("test output"));
         await new Promise((r) => setTimeout(r, 20));

         const outputEvents = events.filter((e) => e.type === "step_output");
         expect(outputEvents.length).toBeGreaterThan(0);
         expect(outputEvents.some((e) => e.output === "test output")).toBe(true);
      });
   });

   describe("propagate to replicas step", () => {
      /** Runs `propagating`/`strict-propagating` up to and through the propagation step. */
      async function runToPropagation(slug: string) {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         const subject = await service.startDeployment(slug, "admin");
         const events: any[] = [];
         subject.subscribe((event) => events.push(JSON.parse((event as any).data)));

         await new Promise((r) => setTimeout(r, 50));
         mockProc.emit("close", 0); // finish the "build" step
         await new Promise((r) => setTimeout(r, 80));

         return events;
      }

      it("runs the propagation service instead of spawning a command", async () => {
         const events = await runToPropagation("propagating");

         expect(replicaPropagation.propagate).toHaveBeenCalledTimes(1);
         const [opts] = replicaPropagation.propagate.mock.calls[0];
         expect(opts.project).toBe("propagating");
         expect(opts.branch).toBe("master");
         expect(opts.triggeredBy).toBe("admin");

         // The step must not have been executed as a shell command.
         const spawnedCmds = (childProcess.spawn as jest.Mock).mock.calls.map((c) => c[0]);
         expect(spawnedCmds).not.toContain("");

         expect(events.some((e) => e.type === "step_start" && e.step === "propagate_replicas")).toBe(true);
         expect(events.some((e) => e.type === "replica_dispatch" && e.step === "propagate_replicas")).toBe(true);
         expect(events.some((e) => e.type === "replica_done" && e.step === "propagate_replicas")).toBe(true);
      });

      it("succeeds and finishes the deployment when no replicas are connected", async () => {
         await runToPropagation("propagating");

         const deployment = await service.getCurrentDeployment();
         expect(deployment?.status).toBe("success");
         expect(deployment?.completedSteps["propagate_replicas"].status).toBe("success");
      });

      it("persists the propagation snapshot on the step so a reload can render it", async () => {
         await runToPropagation("propagating");

         const deployment = await service.getCurrentDeployment();
         const propagation = deployment?.completedSteps["propagate_replicas"].propagation;
         expect(propagation).toBeDefined();
         expect(propagation?.replicas).toEqual([]);
      });

      it("does not fail the pipeline on replica failures by default", async () => {
         replicaPropagation.allSucceeded.mockReturnValue(false);
         replicaPropagation.summarize.mockReturnValue("1/2 replicas deployed");

         await runToPropagation("propagating");

         const deployment = await service.getCurrentDeployment();
         expect(deployment?.status).toBe("success");
         expect(deployment?.completedSteps["propagate_replicas"].status).toBe("success");
      });

      it("fails the pipeline on replica failures when requireAllReplicas is set", async () => {
         replicaPropagation.allSucceeded.mockReturnValue(false);
         replicaPropagation.summarize.mockReturnValue("1/2 replicas deployed");

         const events = await runToPropagation("strict-propagating");

         const deployment = await service.getCurrentDeployment();
         expect(deployment?.status).toBe("failed");
         expect(deployment?.completedSteps["propagate_replicas"].status).toBe("failed");
         expect(deployment?.currentStep.error).toContain("1/2 replicas deployed");

         expect(events.some((e) => e.type === "step_complete" && e.step === "propagate_replicas" && e.status === "failed")).toBe(true);
         expect(emailService.sendEmail).toHaveBeenCalledWith(
            expect.objectContaining({ subject: "Shado Cloud - strict-propagating deployment FAILED" }),
         );
      });

      it("streams replica status and log deltas through the SSE stream", async () => {
         replicaPropagation.propagate.mockImplementation(async (opts: any, cb: any) => {
            const replica = {
               id: "sock1",
               deviceName: "replica-box",
               ip: "1.2.3.4",
               status: "running",
               steps: [{ step: "git_pull", name: "Git Pull", status: "running" }],
               output: "",
            };
            const state: any = { runId: "run1", dispatchedAt: 1, replicas: [replica] };
            cb.onDispatch(state);
            cb.onReplicaUpdate(replica);
            cb.onReplicaOutput("sock1", "Already up to date.\n");
            replica.status = "success";
            cb.onReplicaUpdate(replica);
            state.finishedAt = 2;
            return state;
         });

         const events = await runToPropagation("propagating");

         const updates = events.filter((e) => e.type === "replica_update");
         expect(updates.length).toBe(2);
         expect(updates[0].replica.deviceName).toBe("replica-box");

         const outputs = events.filter((e) => e.type === "replica_output");
         expect(outputs.length).toBe(1);
         expect(outputs[0]).toMatchObject({ replicaId: "sock1", output: "Already up to date.\n" });

         const done = events.find((e) => e.type === "replica_done");
         expect(done.propagation.replicas[0].status).toBe("success");
      });
   });

   describe("build replica image step", () => {
      const imageSteps = [
         { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
         {
            step: "propagate_replicas",
            name: "Propagate to Replicas",
            cmd: "",
            args: [],
            propagateToReplicas: true,
            buildImage: true,
            dockerfile: "../Dockerfile.shado-cloud",
            imageTarget: "runtime",
            imageTag: "shado-cloud:deploy",
            imageService: "shado-cloud",
            smokePort: 9000,
         },
      ];

      beforeEach(() => {
         projectRepo.findOneBy.mockImplementation(({ slug }: any) =>
            Promise.resolve(slug === "imaged" ? makeProject("imaged", imageSteps) : null),
         );
      });

      /** Runs the `imaged` project through its build step and on into propagation. */
      async function runToBuild() {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         const subject = await service.startDeployment("imaged", "admin");
         const events: any[] = [];
         subject.subscribe((event) => events.push(JSON.parse((event as any).data)));

         await new Promise((r) => setTimeout(r, 50));
         mockProc.emit("close", 0); // finish the npm build step
         await new Promise((r) => setTimeout(r, 120));

         return events;
      }

      it("builds, smoke-tests and stages the image, without spawning a command", async () => {
         await runToBuild();

         expect(imageBuilder.build).toHaveBeenCalledTimes(1);
         const [buildOpts] = imageBuilder.build.mock.calls[0];
         expect(buildOpts).toMatchObject({
            dockerfile: "../Dockerfile.shado-cloud",
            target: "runtime",
            tag: "shado-cloud:deploy",
         });

         expect(imageBuilder.smokeTest).toHaveBeenCalledTimes(1);
         expect(imageBuilder.smokeTest.mock.calls[0][0]).toMatchObject({ imageId: "sha256:builtimage", port: 9000 });
         expect(imageBuilder.stage).toHaveBeenCalledWith("sha256:builtimage", expect.any(Function));
      });

      it("hands the staged image to the propagation step", async () => {
         await runToBuild();

         const [propagateOpts] = replicaPropagation.propagate.mock.calls[0];
         expect(propagateOpts.images).toEqual([
            {
               service: "shado-cloud",
               imageId: "sha256:builtimage",
               tarSha256: "a".repeat(64),
               size: 2 * 1024 * 1024 * 1024,
               artifact: "a".repeat(32),
            },
         ]);
      });

      it("fails the deployment when the smoke test rejects the image, and never propagates", async () => {
         imageBuilder.smokeTest.mockRejectedValue(new Error("Image did not become healthy within 90s (HTTP 502)"));

         const events = await runToBuild();

         const deployment = await service.getCurrentDeployment();
         expect(deployment?.status).toBe("failed");
         expect(deployment?.completedSteps["propagate_replicas"].status).toBe("failed");
         expect(deployment?.currentStep.error).toContain("did not become healthy");

         // The whole point of the smoke test: a bad image must not reach a replica.
         expect(imageBuilder.stage).not.toHaveBeenCalled();
         expect(replicaPropagation.propagate).not.toHaveBeenCalled();
         expect(events.some((e) => e.type === "step_complete" && e.step === "propagate_replicas" && e.status === "failed")).toBe(true);
      });

      it("fails with an actionable message when Docker is missing on the host", async () => {
         imageBuilder.build.mockRejectedValue(
            new Error("Docker is not available on this host. Replica images are built here even though this node does not run containers"),
         );

         await runToBuild();

         const deployment = await service.getCurrentDeployment();
         expect(deployment?.status).toBe("failed");
         expect(deployment?.currentStep.error).toContain("Docker is not available");
         expect(replicaPropagation.propagate).not.toHaveBeenCalled();
      });

      it("can stage without smoke testing when the step opts out", async () => {
         const noSmoke = imageSteps.map(s => (s.step === "propagate_replicas" ? { ...s, smokeTest: false } : s));
         projectRepo.findOneBy.mockImplementation(({ slug }: any) =>
            Promise.resolve(slug === "imaged" ? makeProject("imaged", noSmoke) : null),
         );

         await runToBuild();

         expect(imageBuilder.smokeTest).not.toHaveBeenCalled();
         expect(imageBuilder.stage).toHaveBeenCalledTimes(1);
      });

      it("builds from the primary's own working directory when no sourceRepo is set", async () => {
         await runToBuild();

         expect(imageBuilder.cloneSource).not.toHaveBeenCalled();
         // makeProject uses workDir "__CWD__", which resolveWorkDir expands to process.cwd().
         expect(imageBuilder.build.mock.calls[0][0].workDir).toBe(process.cwd());
      });

      describe("building from a fresh clone", () => {
         const clonedSteps = imageSteps.map(s =>
            s.step === "propagate_replicas"
               ? {
                    ...s,
                    sourceRepo: "git@github.com:Shado-Cloud/Shado-Cloud-Services.git",
                    sourceBranch: "main",
                    contextSubdir: "shado-cloud",
                 }
               : s,
         );
         let dispose: jest.Mock;

         beforeEach(() => {
            dispose = jest.fn();
            imageBuilder.cloneSource.mockResolvedValue({ dir: "/tmp/shado-build-xyz", dispose });
            projectRepo.findOneBy.mockImplementation(({ slug }: any) =>
               Promise.resolve(slug === "imaged" ? makeProject("imaged", clonedSteps) : null),
            );
         });

         it("clones the configured repo and builds from the given subdirectory", async () => {
            await runToBuild();

            expect(imageBuilder.cloneSource).toHaveBeenCalledWith(
               "git@github.com:Shado-Cloud/Shado-Cloud-Services.git",
               "main",
               expect.any(Function),
               ["shado-cloud"],
            );
            // Context is the subdirectory of the clone, NOT the primary's own checkout — that is
            // the whole point: the image must not pick up this host's config.yml or node_modules.
            expect(imageBuilder.build.mock.calls[0][0].workDir).toBe("/tmp/shado-build-xyz/shado-cloud");
            expect(imageBuilder.smokeTest.mock.calls[0][0].workDir).toBe("/tmp/shado-build-xyz/shado-cloud");
         });

         /*
          * The build context is a submodule, and NOTHING in this project advances the
          * superproject's gitlinks — each service deploys itself with a `git pull` inside its own
          * submodule directory. So the submodule to build must be named to cloneSource, which
          * checks it out at its branch tip; otherwise the build silently used whatever commit the
          * pointer was parked on. That produced images four commits behind master that built
          * clean and passed the smoke test, so the only visible symptom was a fix that appeared to
          * have no effect on the replica.
          */
         it("names the context submodule so it is checked out at its branch tip, not the gitlink", async () => {
            await runToBuild();
            expect(imageBuilder.cloneSource.mock.calls[0][3]).toEqual(["shado-cloud"]);
         });

         it("deletes the clone after a successful build", async () => {
            await runToBuild();
            expect(dispose).toHaveBeenCalledTimes(1);
         });

         it("deletes the clone even when the build fails", async () => {
            imageBuilder.build.mockRejectedValue(new Error("docker build exited with code 1"));

            await runToBuild();

            // Otherwise a few hundred MB would accumulate in the temp dir on every failed deploy.
            expect(dispose).toHaveBeenCalledTimes(1);
            expect((await service.getCurrentDeployment())?.status).toBe("failed");
         });

         it("deletes the clone even when the smoke test rejects the image", async () => {
            imageBuilder.smokeTest.mockRejectedValue(new Error("Image did not become healthy"));

            await runToBuild();

            expect(dispose).toHaveBeenCalledTimes(1);
            expect(imageBuilder.stage).not.toHaveBeenCalled();
         });

         it("falls back to the project's branch when sourceBranch is unset", async () => {
            const noBranch = clonedSteps.map(s => (s.step === "propagate_replicas" ? { ...s, sourceBranch: undefined } : s));
            projectRepo.findOneBy.mockImplementation(({ slug }: any) =>
               Promise.resolve(slug === "imaged" ? makeProject("imaged", noBranch) : null),
            );

            await runToBuild();

            // makeProject sets branch "master".
            expect(imageBuilder.cloneSource).toHaveBeenCalledWith(expect.any(String), "master", expect.any(Function), ["shado-cloud"]);
         });

         it("fails the step, without building, when the clone fails", async () => {
            imageBuilder.cloneSource.mockRejectedValue(
               new Error("Could not clone git@github.com:Shado-Cloud/Shado-Cloud-Services.git (main): authentication failed"),
            );

            await runToBuild();

            expect(imageBuilder.build).not.toHaveBeenCalled();
            const deployment = await service.getCurrentDeployment();
            expect(deployment?.status).toBe("failed");
            expect(deployment?.currentStep.error).toContain("Could not clone");
         });
      });
   });

   describe("default step reconciliation", () => {
      /**
       * seedDefaults used to only INSERT projects, so a step added to the defaults never reached an
       * environment where the project already existed — and it could not be shipped by migration
       * either, since migrations/*.ts is gitignored. That presented as a pipeline silently missing
       * a step, which is how "Propagate to Replicas" ended up spinning with no image to send.
       */
      it("adds a missing default step to an existing project", async () => {
         const bare = backendSteps.filter(s => s.step !== "propagate_replicas");
         const stored = makeProject("backend", bare);
         projectRepo.findOneBy.mockResolvedValue(stored);

         await service.onModuleInit();

         expect(projectRepo.save).toHaveBeenCalled();
         const saved = projectRepo.save.mock.calls.at(-1)[0] as DeploymentProject;
         expect(saved.getSteps().map(s => s.step)).toContain("propagate_replicas");
      });

      it("inserts it in the position the defaults give it", async () => {
         const bare = backendSteps.filter(s => s.step !== "propagate_replicas");
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", bare));

         await service.onModuleInit();

         const ids = (projectRepo.save.mock.calls.at(-1)[0] as DeploymentProject).getSteps().map(s => s.step);
         // Last: replicas only update once this node has restarted and verified itself.
         expect(ids.indexOf("propagate_replicas")).toBe(ids.length - 1);
         expect(ids.indexOf("propagate_replicas")).toBeGreaterThan(ids.indexOf("verify"));
      });

      it("fills in build settings a bare propagation step is missing", async () => {
         // The case that broke a real upgrade: an existing project already had propagate_replicas
         // from before it could build, so adding steps alone would leave it trying to build with
         // nothing configured.
         const barePropagate = backendSteps.map(s =>
            s.step === "propagate_replicas" ? { step: s.step, name: s.name, cmd: "", args: [], propagateToReplicas: true } : s,
         );
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", barePropagate));

         await service.onModuleInit();

         const step = (projectRepo.save.mock.calls.at(-1)[0] as DeploymentProject)
            .getSteps().find(s => s.step === "propagate_replicas");
         expect(step?.buildImage).toBe(true);
         expect(step?.sourceRepo).toContain("Shado-Cloud-Services");
         expect(step?.contextSubdir).toBe("shado-cloud");
         expect(step?.dockerfile).toBe("../Dockerfile.shado-cloud");
      });

      it("does not overwrite a build setting the operator already chose", async () => {
         const customised = backendSteps.map(s =>
            s.step === "propagate_replicas" ? { ...s, imageTag: "mine:custom", buildImage: false } : s,
         );
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", customised));

         await service.onModuleInit();

         expect(projectRepo.save).not.toHaveBeenCalled();
      });

      it("preserves customised commands and skip flags on existing steps", async () => {
         const customised = backendSteps
            .filter(s => s.step !== "propagate_replicas")
            .map(s => (s.step === "test" ? { ...s, skip: true, args: ["test", "--custom"] } : s));
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", customised));

         await service.onModuleInit();

         const steps = (projectRepo.save.mock.calls.at(-1)[0] as DeploymentProject).getSteps();
         const test = steps.find(s => s.step === "test");
         expect(test?.skip).toBe(true);
         expect(test?.args).toEqual(["test", "--custom"]);
      });

      it("rewrites a source repo this code previously seeded with the wrong scheme", async () => {
         // An HTTPS clone needs a username and token, and unattended there is no terminal to
         // supply them — field-level merging cannot fix this, since the field is already present.
         const legacy = backendSteps.map(s =>
            s.step === "propagate_replicas"
               ? { ...s, sourceRepo: "https://github.com/Shado-Cloud/Shado-Cloud-Services.git" }
               : s,
         );
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", legacy));

         await service.onModuleInit();

         const step = (projectRepo.save.mock.calls.at(-1)[0] as DeploymentProject)
            .getSteps().find(s => s.step === "propagate_replicas");
         expect(step?.sourceRepo).toBe("git@github.com:Shado-Cloud/Shado-Cloud-Services.git");
      });

      it("leaves a source repo the operator chose alone", async () => {
         // Only an exact prior default is replaced, so a deliberate choice is never overwritten.
         const custom = backendSteps.map(s =>
            s.step === "propagate_replicas" ? { ...s, sourceRepo: "git@git.internal:me/mirror.git" } : s,
         );
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", custom));

         await service.onModuleInit();

         expect(projectRepo.save).not.toHaveBeenCalled();
      });

      it("does nothing when the project already has every default step", async () => {
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", backendSteps));

         await service.onModuleInit();

         expect(projectRepo.save).not.toHaveBeenCalled();
      });

      it("does not re-add a step the operator deleted after reconciliation ran", async () => {
         // Recorded at or beyond the current version, so a deliberate deletion stays deleted
         // rather than reappearing on every boot. Deliberately high rather than the literal
         // current version, so bumping DEFAULT_STEPS_VERSION does not silently break this test —
         // a bump SHOULD reconcile again, which is a different case.
         redisHashes[REDIS_STEPS_VERSION_KEY] = { backend: "99" };
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", backendSteps.filter(s => s.step !== "propagate_replicas")));

         await service.onModuleInit();

         expect(projectRepo.save).not.toHaveBeenCalled();
      });

      it("leaves unparseable step config alone rather than corrupting it", async () => {
         const broken = makeProject("backend", backendSteps);
         broken.steps = "{not json";
         projectRepo.findOneBy.mockResolvedValue(broken);

         await service.onModuleInit();

         expect(projectRepo.save).not.toHaveBeenCalled();
      });
   });

   describe("getSubject", () => {
      it("should return null when no deployment", () => {
         expect(service.getSubject()).toBeNull();
      });

      it("should return subject during deployment", async () => {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         await service.startDeployment("backend", "test");

         expect(service.getSubject()).not.toBeNull();
      });
   });
});
