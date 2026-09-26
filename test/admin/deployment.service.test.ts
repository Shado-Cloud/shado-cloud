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
import * as path from "path";
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
      // Mirrors the KEY SET of the current default step, which is what reconciliation compares —
      // a missing key here would make the "does nothing" tests below fail for the wrong reason.
      services: [
         {
            service: "shado-cloud",
            contextSubdir: "shado-cloud",
            dockerfile: "../Dockerfile.shado-cloud",
            imageTarget: "runtime",
            imageTag: "shado-cloud:deploy",
            smokePort: 9000,
         },
      ],
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
         // Staged artifacts are present by default; tests that care about pruning override it.
         hasStagedArtifact: jest.fn().mockReturnValue(true),
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
               // Carried so the replica's updater gates the swap on the SAME target the smoke test
               // just probed, instead of guessing which port belongs to the container it recreated.
               healthPort: 9000,
               healthPath: undefined,
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

   describe("building every service for a failover replica", () => {
      const multiSteps = [
         { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
         {
            step: "propagate_replicas",
            name: "Propagate to Replicas",
            cmd: "",
            args: [],
            propagateToReplicas: true,
            buildImage: true,
            sourceRepo: "git@github.com:Shado-Cloud/Shado-Cloud-Services.git",
            sourceBranch: "main",
            services: [
               { service: "shado-cloud", contextSubdir: "shado-cloud", dockerfile: "../Dockerfile.shado-cloud", imageTarget: "runtime", smokePort: 9000 },
               { service: "shado-auth-api", contextSubdir: "shado-auth-api", dockerfile: "../Dockerfile.nestjs", imageTarget: "runtime", smokePort: 11001, smokeTest: false },
               { service: "shado-cloud-frontend", contextSubdir: "shado-cloud-frontend", dockerfile: "../Dockerfile.sveltekit", imageTarget: "runtime", smokePort: 80, envFile: "__CWD__/../shado-cloud-frontend/.env" },
            ],
         },
      ];
      let disposeEnv: jest.Mock;

      beforeEach(() => {
         disposeEnv = jest.fn();
         imageBuilder.stageEnvFile = jest.fn().mockReturnValue(disposeEnv);
         // A distinct image per build, so the per-service refs can be told apart.
         let n = 0;
         imageBuilder.build.mockImplementation(async () => `sha256:image${++n}`);
         imageBuilder.stage.mockImplementation(async (imageId: string) => ({
            artifact: imageId.slice(-1).repeat(32),
            imageId,
            tarSha256: imageId.slice(-1).repeat(64),
            size: 1024 * 1024,
         }));
         projectRepo.findOneBy.mockImplementation(({ slug }: any) =>
            Promise.resolve(slug === "multi" ? makeProject("multi", multiSteps) : null),
         );
      });

      async function runMulti() {
         const mockProc = createMockProc();
         mockSpawnWithPwd(mockProc);
         const subject = await service.startDeployment("multi", "admin");
         const events: any[] = [];
         subject.subscribe((event) => events.push(JSON.parse((event as any).data)));
         await new Promise((r) => setTimeout(r, 50));
         mockProc.emit("close", 0);
         await new Promise((r) => setTimeout(r, 200));
         return events;
      }

      it("builds and stages one image per configured service", async () => {
         await runMulti();

         expect(imageBuilder.build).toHaveBeenCalledTimes(3);
         expect(imageBuilder.stage).toHaveBeenCalledTimes(3);

         const [propagateOpts] = replicaPropagation.propagate.mock.calls[0];
         expect(propagateOpts.images.map((i: any) => i.service)).toEqual([
            "shado-cloud", "shado-auth-api", "shado-cloud-frontend",
         ]);
      });

      /*
       * One clone, not one per service. Each service is a submodule of the superproject, so cloning
       * once costs a single fetch instead of N — and, more importantly, guarantees every image in a
       * deployment comes from the same moment in the branch's history rather than from N fetches
       * that could straddle a push.
       */
      it("clones once and names every context submodule", async () => {
         await runMulti();

         expect(imageBuilder.cloneSource).toHaveBeenCalledTimes(1);
         expect(imageBuilder.cloneSource.mock.calls[0][3]).toEqual([
            "shado-cloud", "shado-auth-api", "shado-cloud-frontend",
         ]);
         // Each build context is its own subdirectory of that one clone.
         expect(imageBuilder.build.mock.calls.map((c: any[]) => c[0].workDir)).toEqual([
            "/tmp/shado-build-xyz/shado-cloud",
            "/tmp/shado-build-xyz/shado-auth-api",
            "/tmp/shado-build-xyz/shado-cloud-frontend",
         ]);
      });

      it("carries each service's health target to the replica", async () => {
         await runMulti();

         const [propagateOpts] = replicaPropagation.propagate.mock.calls[0];
         expect(propagateOpts.images.map((i: any) => [i.service, i.healthPort])).toEqual([
            ["shado-cloud", 9000],
            ["shado-auth-api", 11001],
            ["shado-cloud-frontend", 80],
         ]);
      });

      it("honours per-service smoke settings", async () => {
         await runMulti();

         // auth-api opts out (it needs a database to boot); the other two are probed.
         expect(imageBuilder.smokeTest).toHaveBeenCalledTimes(2);
         expect(imageBuilder.smokeTest.mock.calls.map((c: any[]) => c[0].port)).toEqual([9000, 80]);
      });

      describe("build-time env for the frontends", () => {
         it("stages the env file into the frontend's context and removes it afterwards", async () => {
            await runMulti();

            expect(imageBuilder.stageEnvFile).toHaveBeenCalledTimes(1);
            const [src, contextDir] = imageBuilder.stageEnvFile.mock.calls[0];
            // __CWD__ expands against the primary's checkout: the values live on THIS host,
            // deliberately not in the repository.
            expect(src).toBe(path.resolve(process.cwd(), "../shado-cloud-frontend/.env"));
            expect(contextDir).toBe("/tmp/shado-build-xyz/shado-cloud-frontend");
            // Removed whatever happens, so it cannot be picked up by a later build.
            expect(disposeEnv).toHaveBeenCalledTimes(1);
         });

         it("removes the staged env file even when that service's build fails", async () => {
            imageBuilder.build.mockImplementation(async (opts: any) => {
               if (opts.workDir.endsWith("shado-cloud-frontend")) throw new Error("vite build failed");
               return "sha256:ok";
            });

            await runMulti();

            expect(disposeEnv).toHaveBeenCalledTimes(1);
         });

         /*
          * A frontend built with no env produces a bundle that loads and then fails every request,
          * which is far harder to diagnose than a failed build — so a missing env file must stop
          * the step rather than be skipped.
          */
         it("fails the step when the env file is missing on this host", async () => {
            imageBuilder.stageEnvFile.mockImplementation(() => {
               throw new Error("Env file /nope/.env does not exist on this host.");
            });

            await runMulti();

            const deployment = await service.getCurrentDeployment();
            expect(deployment?.status).toBe("failed");
            expect(deployment?.currentStep.error).toContain("does not exist on this host");
            expect(replicaPropagation.propagate).not.toHaveBeenCalled();
         });
      });

      /*
       * Fail fast across the set. A half-applied stack is worse than none: the replica would be left
       * running a mix of old and new services with no record of which, so nothing is dispatched
       * unless every image is built and staged.
       */
      it("dispatches nothing when any one service fails to build", async () => {
         imageBuilder.build.mockImplementation(async (opts: any) => {
            if (opts.workDir.endsWith("shado-auth-api")) throw new Error("docker build exited with code 1");
            return "sha256:ok";
         });

         await runMulti();

         const deployment = await service.getCurrentDeployment();
         expect(deployment?.status).toBe("failed");
         // Names the service. With one image the step WAS the service; across eight builds a bare
         // "exited with code 1" says nothing about where to look.
         expect(deployment?.currentStep.error).toContain("shado-auth-api:");
         expect(replicaPropagation.propagate).not.toHaveBeenCalled();
      });

      it("removes the shared clone after a failure part-way through the set", async () => {
         const dispose = jest.fn();
         imageBuilder.cloneSource.mockResolvedValue({ dir: "/tmp/shado-build-xyz", dispose });
         imageBuilder.build.mockImplementation(async (opts: any) => {
            if (opts.workDir.endsWith("shado-auth-api")) throw new Error("boom");
            return "sha256:ok";
         });

         await runMulti();

         expect(dispose).toHaveBeenCalledTimes(1);
      });

      /*
       * A step written before multi-service support has no `services` array. It must keep building
       * exactly the one image it always did — these are hand-edited in the admin UI, so a silent
       * change of meaning would be invisible until a replica ran the wrong thing.
       */
      it("still builds a single image from the legacy fields when services is absent", async () => {
         const legacy = [
            { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
            {
               step: "propagate_replicas", name: "Propagate to Replicas", cmd: "", args: [],
               propagateToReplicas: true, buildImage: true,
               dockerfile: "../Dockerfile.shado-cloud", imageTarget: "runtime",
               imageTag: "shado-cloud:deploy", imageService: "shado-cloud", smokePort: 9000,
            },
         ];
         projectRepo.findOneBy.mockImplementation(({ slug }: any) =>
            Promise.resolve(slug === "multi" ? makeProject("multi", legacy) : null),
         );

         await runMulti();

         expect(imageBuilder.build).toHaveBeenCalledTimes(1);
         expect(imageBuilder.build.mock.calls[0][0]).toMatchObject({ tag: "shado-cloud:deploy", target: "runtime" });
         const [propagateOpts] = replicaPropagation.propagate.mock.calls[0];
         expect(propagateOpts.images.map((i: any) => i.service)).toEqual(["shado-cloud"]);
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
         expect(step?.services?.length).toBeGreaterThan(1);
      });

      /*
       * The point of the v4 bump: an install that predates multi-service support builds shado-cloud
       * alone, which is not enough for a replica to take over. `services` is absent from such a
       * step, so field-level merging delivers the whole list.
       */
      it("gives an existing pipeline the full service list, not just shado-cloud", async () => {
         const singleImage = backendSteps.map(s =>
            s.step === "propagate_replicas"
               ? {
                    step: s.step, name: s.name, cmd: "", args: [], propagateToReplicas: true,
                    buildImage: true, sourceRepo: s.sourceRepo, sourceBranch: "main",
                    contextSubdir: "shado-cloud", dockerfile: "../Dockerfile.shado-cloud",
                    imageTarget: "runtime", imageTag: "shado-cloud:deploy", imageService: "shado-cloud",
                    smokePort: 9000,
                 }
               : s,
         );
         projectRepo.findOneBy.mockResolvedValue(makeProject("backend", singleImage));

         await service.onModuleInit();

         const step = (projectRepo.save.mock.calls.at(-1)[0] as DeploymentProject)
            .getSteps().find(s => s.step === "propagate_replicas");
         const services = step?.services ?? [];
         expect(services.map(s => s.service)).toEqual(
            expect.arrayContaining([
               "shado-cloud", "shado-auth-api", "shado-metrics", "shado-music-api",
               "shado-gym-api", "shado-cloud-frontend", "shado-music-frontend", "shado-gym-app",
            ]),
         );
         // Frontends bake their config at build time and their .env is gitignored, so a clean clone
         // cannot build them without a value supplied from this host.
         expect(services.find(s => s.service === "shado-cloud-frontend")?.envFile).toContain(".env");
         // The legacy fields survive untouched but go inert — serviceSpecs() prefers `services`.
         expect(step?.imageService).toBe("shado-cloud");
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

   describe("manual propagation (startPropagation)", () => {
      const staged = {
         service: "shado-cloud",
         imageId: "sha256:earlier",
         tarSha256: "b".repeat(64),
         size: 1024 * 1024 * 1024,
         artifact: "b".repeat(32),
      };

      /** Records a finished deployment that staged `images`, as a real run would. */
      function seedLastDeployment(images: any[]) {
         redisStore["deployment:last"] = JSON.stringify({
            id: "deploy_1",
            project: "propagating",
            status: "success",
            currentStep: { step: "propagate_replicas", status: "success", output: "" },
            completedSteps: {},
            startedAt: new Date().toISOString(),
            triggeredBy: "admin",
            images,
         });
      }

      it("runs the propagation step without the pipeline in front of it", async () => {
         mockSpawnWithPwd(createMockProc());

         await service.startPropagation("propagating", "admin");
         await new Promise((r) => setTimeout(r, 80));

         expect(replicaPropagation.propagate).toHaveBeenCalledTimes(1);
         // "build" precedes propagation in this project's pipeline and must NOT have run: the
         // point is to reach the replicas without touching the primary.
         const spawnedCmds = (childProcess.spawn as jest.Mock).mock.calls.map((c) => c[0]);
         expect(spawnedCmds).not.toContain("npm");
      });

      it("completes as its own deployment record", async () => {
         mockSpawnWithPwd(createMockProc());

         await service.startPropagation("propagating", "admin");
         await new Promise((r) => setTimeout(r, 80));

         const deployment = await service.getCurrentDeployment();
         expect(deployment?.id).toMatch(/^propagate_/);
         expect(deployment?.status).toBe("success");
         expect(deployment?.completedSteps["propagate_replicas"].status).toBe("success");
      });

      it("emits the propagation SSE events a client renders", async () => {
         mockSpawnWithPwd(createMockProc());

         const subject = await service.startPropagation("propagating", "admin");
         const events: any[] = [];
         subject.subscribe((event) => events.push(JSON.parse((event as any).data)));
         await new Promise((r) => setTimeout(r, 80));

         expect(events.some((e) => e.type === "step_start" && e.step === "propagate_replicas")).toBe(true);
         expect(events.some((e) => e.type === "replica_dispatch")).toBe(true);
         expect(events.some((e) => e.type === "deployment_complete")).toBe(true);
      });

      it("throws when the project has no propagation step", async () => {
         await expect(service.startPropagation("frontend", "admin")).rejects.toThrow(
            'Project "frontend" has no "Propagate to Replicas" step',
         );
      });

      it("throws when the project does not exist", async () => {
         await expect(service.startPropagation("nonexistent", "admin")).rejects.toThrow("not found");
      });

      it("throws when a deployment is already running", async () => {
         mockSpawnWithPwd(createMockProc());
         await service.startDeployment("backend", "admin");

         await expect(service.startPropagation("backend", "admin")).rejects.toThrow("Deployment already in progress");
      });

      /*
       * A propagation with no replicas online SUCCEEDS — correctly, since a replica-less install
       * must not fail every deployment. That also means retryStep, which needs a failed deployment
       * and a failed step, has nothing to retry. This button is the only route back for a replica
       * that was merely asleep, so it must run regardless of the step's skip flag: honouring it
       * would give an operator a button that reports success having done nothing.
       */
      it("runs even when the step is flagged to be skipped", async () => {
         const skipped = propagateSteps.map((s) => (s.propagateToReplicas ? { ...s, skip: true } : s));
         projectRepo.findOneBy.mockImplementation(({ slug }: any) =>
            Promise.resolve(slug === "propagating" ? makeProject("propagating", skipped) : null),
         );
         mockSpawnWithPwd(createMockProc());

         await service.startPropagation("propagating", "admin");
         await new Promise((r) => setTimeout(r, 80));

         expect(replicaPropagation.propagate).toHaveBeenCalledTimes(1);
         expect((await service.getCurrentDeployment())?.completedSteps["propagate_replicas"].status).toBe("success");
      });

      describe("reusing an already-staged image", () => {
         const imageSteps = [
            { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
            {
               step: "propagate_replicas",
               name: "Propagate to Replicas",
               cmd: "",
               args: [],
               propagateToReplicas: true,
               buildImage: true,
               sourceRepo: "git@github.com:Shado-Cloud/Shado-Cloud-Services.git",
               contextSubdir: "shado-cloud",
               imageService: "shado-cloud",
            },
         ];

         beforeEach(() => {
            projectRepo.findOneBy.mockImplementation(({ slug }: any) =>
               Promise.resolve(slug === "propagating" ? makeProject("propagating", imageSteps) : null),
            );
         });

         it("dispatches the staged image without cloning or rebuilding", async () => {
            seedLastDeployment([staged]);
            mockSpawnWithPwd(createMockProc());

            await service.startPropagation("propagating", "admin", { reuseImage: true });
            await new Promise((r) => setTimeout(r, 80));

            expect(imageBuilder.cloneSource).not.toHaveBeenCalled();
            expect(imageBuilder.build).not.toHaveBeenCalled();
            expect(replicaPropagation.propagate.mock.calls[0][0].images).toEqual([staged]);
         });

         it("rebuilds when reuse is not requested", async () => {
            seedLastDeployment([staged]);
            mockSpawnWithPwd(createMockProc());

            await service.startPropagation("propagating", "admin");
            await new Promise((r) => setTimeout(r, 120));

            expect(imageBuilder.build).toHaveBeenCalledTimes(1);
            expect(replicaPropagation.propagate.mock.calls[0][0].images).toEqual([
               expect.objectContaining({ imageId: "sha256:builtimage" }),
            ]);
         });

         /*
          * Artifacts are pruned on a TTL and live in the OS temp dir, so an image recorded by a
          * past deployment is not evidence the bytes are still there. Refused here rather than
          * dispatched, because the alternative — falling through with no images — would order the
          * replica down the source-deployment path instead, which is a different operation
          * entirely and not what was asked for.
          */
         it("refuses when the staged artifact has been pruned", async () => {
            seedLastDeployment([staged]);
            imageBuilder.hasStagedArtifact.mockReturnValue(false);

            await expect(service.startPropagation("propagating", "admin", { reuseImage: true })).rejects.toThrow(
               "No image from a previous run is still staged",
            );
            expect(replicaPropagation.propagate).not.toHaveBeenCalled();
         });

         it("refuses when no previous run staged anything", async () => {
            await expect(service.startPropagation("propagating", "admin", { reuseImage: true })).rejects.toThrow(
               "No image from a previous run is still staged",
            );
         });
      });

      describe("availableStagedImages", () => {
         it("is empty with no deployment history", async () => {
            expect(await service.availableStagedImages()).toEqual([]);
         });

         it("reports images whose artifacts are still on disk", async () => {
            seedLastDeployment([staged]);
            expect(await service.availableStagedImages()).toEqual([staged]);
            expect(imageBuilder.hasStagedArtifact).toHaveBeenCalledWith(staged.artifact);
         });

         it("drops images whose artifacts are gone", async () => {
            seedLastDeployment([staged]);
            imageBuilder.hasStagedArtifact.mockReturnValue(false);
            expect(await service.availableStagedImages()).toEqual([]);
         });
      });

      describe("getPropagationStep", () => {
         it("returns the step for a project that has one", async () => {
            const step = await service.getPropagationStep("propagating");
            expect(step?.step).toBe("propagate_replicas");
         });

         it("returns null for a project without one", async () => {
            expect(await service.getPropagationStep("frontend")).toBeNull();
         });

         it("returns null for an unknown project", async () => {
            expect(await service.getPropagationStep("nonexistent")).toBeNull();
         });
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

   describe("getStream (a client joining a deployment in progress)", () => {
      it("should return null when no deployment", () => {
         expect(service.getStream()).toBeNull();
      });

      it("opens with a snapshot holding the output emitted before the client connected, then streams on with nothing dropped", async () => {
         // The scenario behind "no build logs until the step fails": the primary restarts mid-
         // pipeline, the UI reconnects to /deployment/stream while a step is already running, and
         // Redis only has that step's output as of its start. The stream must catch the client up.
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         await service.startDeployment("backend", "test");
         await new Promise((r) => setTimeout(r, 50));
         mockProc.stdout.emit("data", Buffer.from("emitted before the client joined\n"));

         const events: any[] = [];
         service.getStream()!.subscribe((event) => events.push(JSON.parse((event as any).data)));
         mockProc.stdout.emit("data", Buffer.from("emitted after\n"));

         expect(events[0].type).toBe("snapshot");
         expect(events[0].deployment.status).toBe("running");
         expect(events[0].deployment.currentStep.step).toBe("git_pull");
         expect(events[0].deployment.currentStep.output).toContain("emitted before the client joined\n");
         // The live tail follows directly, and the snapshot doesn't already contain it.
         expect(events[0].deployment.currentStep.output).not.toContain("emitted after");
         expect(events.slice(1)).toEqual([{ type: "step_output", step: "git_pull", output: "emitted after\n" }]);
      });

      it("takes the snapshot per subscriber, at the moment each one connects", async () => {
         const mockProc = new EventEmitter() as any;
         mockProc.stdout = new EventEmitter();
         mockProc.stderr = new EventEmitter();
         mockSpawnWithPwd(mockProc);

         await service.startDeployment("backend", "test");
         await new Promise((r) => setTimeout(r, 50));
         const stream = service.getStream()!;

         mockProc.stdout.emit("data", Buffer.from("A\n"));
         const first: any[] = [];
         stream.subscribe((e) => first.push(JSON.parse((e as any).data)));
         mockProc.stdout.emit("data", Buffer.from("B\n"));
         const second: any[] = [];
         stream.subscribe((e) => second.push(JSON.parse((e as any).data)));

         expect(first[0].deployment.currentStep.output).toMatch(/A\n$/);
         expect(second[0].deployment.currentStep.output).toMatch(/A\nB\n$/);
      });
   });
});
