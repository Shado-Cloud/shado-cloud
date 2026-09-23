import { Injectable, Inject, OnModuleInit, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { spawn } from "child_process";
import * as path from "path";
import { Subject } from "rxjs";
import { EnvVariables } from "src/config/config.validator";
import { EmailService } from "./email.service";
import { FeatureFlagService } from "./feature-flag.service";
import { FeatureFlagNamespace } from "src/models/admin/featureFlag";
import type Redis from "ioredis";
import { REDIS_CACHE } from "src/util";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { DeploymentProject, DeploymentStepConfig, ReplicaServiceBuildSpec } from "src/models/admin/deploymentProject";
import { ReplicaPropagationService, type ReplicaPropagationState, type ReplicaRunState } from "./replica-propagation.service";
import { ImageBuildService } from "./image-build.service";
import type { ReplicaImageRef } from "src/replication/replica-link.constants";

export type StepStatus = "pending" | "running" | "success" | "failed" | "skipped";

export interface StepState {
   step: string;
   status: StepStatus;
   output: string;
   startedAt?: Date;
   finishedAt?: Date;
   error?: string;
   attempt?: number;
   maxAttempts?: number;
   /** Set only on a `propagateToReplicas` step: per-replica status and logs. */
   propagation?: ReplicaPropagationState;
}

export interface DeploymentState {
   id: string;
   project: string;
   status: "running" | "success" | "failed";
   currentStep: StepState;
   completedSteps: Record<string, StepState>;
   startedAt: Date;
   finishedAt?: Date;
   triggeredBy: string;
   /**
    * Images staged by `buildImage` steps during this run, handed to replicas by a later
    * `propagateToReplicas` step. Persisted with the state so a resume after the primary's own
    * restart still knows what to propagate.
    */
   images?: ReplicaImageRef[];
}

interface DeploymentEvent {
   type:
      | "step_start"
      | "step_output"
      | "step_complete"
      | "deployment_complete"
      /** Full propagation snapshot: replica list with their pipelines and accept/reject state. */
      | "replica_dispatch"
      /** One replica's status/step transition. */
      | "replica_update"
      /** A log delta for one replica. */
      | "replica_output"
      /** Terminal propagation snapshot. */
      | "replica_done"
      /** The propagation step finished building and staging its image. */
      | "replica_image_staged";
   step?: string;
   output?: string;
   status?: StepStatus;
   error?: string;
   startedAt?: Date;
   finishedAt?: Date;
   deployment?: DeploymentState;
   attempt?: number;
   maxAttempts?: number;
   /** replica_dispatch / replica_done */
   propagation?: ReplicaPropagationState;
   /** replica_update */
   replica?: ReplicaRunState;
   /** replica_output */
   replicaId?: string;
   /**
    * Images staged so far. Sent with a buildImage step's completion so a client watching the SSE
    * stream can show the result immediately — it would otherwise not learn about them until
    * deployment_complete or a reload, since only deltas travel while a run is in progress.
    */
   images?: ReplicaImageRef[];
}

const REDIS_KEY_CURRENT = "deployment:current";
const REDIS_KEY_LAST = "deployment:last";
const REDIS_KEY_QUEUE = "deployment:queue";
/** Which default-step additions have been reconciled into each seeded project. */
const REDIS_KEY_STEPS_VERSION = "deployment:seeded-steps-version";

/**
 * Bump when adding a step to DEFAULT_PROJECTS that existing installs should also receive.
 *
 * `seedDefaults` only ever INSERTED projects, so a step added to the defaults never reached an
 * environment where the project already existed — and it could not be delivered by migration
 * either, since `migrations/*.ts` is gitignored in this repo. New steps therefore had to be
 * hand-added in the admin UI on every environment, which is exactly the kind of manual step that
 * gets forgotten and then presents as a pipeline that silently does nothing.
 *
 * v4 adds `services` to the propagation step, so an existing pipeline starts building every
 * service rather than shado-cloud alone. Field-level merging delivers it because `services` is
 * absent from an older step; the legacy single-image fields stay on the row but go inert, since
 * `serviceSpecs()` prefers `services` whenever it is set.
 */
const DEFAULT_STEPS_VERSION = 4;

/**
 * SSH rather than HTTPS. An HTTPS clone needs a username and token, and running unattended there
 * is no terminal to supply them — git then fails with "could not read Username ... No such device
 * or address", which describes the missing TTY rather than the missing credential. SSH uses the
 * service user's key, and .gitmodules' HTTPS URLs are rewritten at clone time.
 */
const SERVICES_REPO_SSH = "git@github.com:Shado-Cloud/Shado-Cloud-Services.git";

/**
 * Corrections to values THIS CODE previously seeded, applied once per version bump.
 *
 * Field-level reconciliation only fills in what is absent, so it cannot repair a default that was
 * wrong when it was written. These replace an exact prior default and nothing else — a value the
 * operator chose never matches, so it is never touched.
 */
const LEGACY_STEP_VALUE_FIXUPS: { step: string; field: keyof DeploymentStepConfig; from: string; to: string }[] = [
   {
      step: "propagate_replicas",
      field: "sourceRepo",
      from: "https://github.com/Shado-Cloud/Shado-Cloud-Services.git",
      to: SERVICES_REPO_SSH,
   },
];

/**
 * A sibling package of the primary's own checkout, e.g. `__CWD__/../shado-auth-api`.
 * `resolveWorkDir` expands the `__CWD__` prefix at runtime, so these entries work both in a
 * dev workspace and on a host where the repos sit next to each other.
 */
const sibling = (dir: string): string => `__CWD__/../${dir}`;

/**
 * Every service image delivered to a replica, in build order.
 *
 * All of them, not just shado-cloud, because a replica that can TAKE OVER has to be running the
 * whole stack — a mirror needs the file tree, a failover node needs the services. They are built
 * from one shared clone of the superproject, each from its own submodule.
 *
 * `service` must match a compose service on the replica: it is what the replica's updater looks up
 * to know which container to recreate, and what its per-service health gate resolves.
 *
 * ── On smoke tests ──
 * The smoke test boots the image with no dependencies reachable and probes /health, so a broken
 * image is caught on the primary rather than on a remote host that then fails to come back up.
 *
 * That works for shado-cloud (with `role: replica` it boots ReplicationModule, which needs no
 * database) and for the frontends (nginx serving static files needs nothing at all). It does NOT
 * work for the other four APIs: they boot AppModule, which connects to MySQL and Redis during
 * startup, so a dependency-less container exits before it can answer. They are therefore staged
 * unverified, and the safety net for them is the replica updater's per-service health gate, which
 * rolls each service back to its previous image if it does not come up. Giving them a throwaway
 * dependency stack at smoke time would restore the earlier check and is worth doing.
 */
const REPLICA_SERVICES: ReplicaServiceBuildSpec[] = [
   {
      service: "shado-cloud",
      contextSubdir: "shado-cloud",
      dockerfile: "../Dockerfile.shado-cloud",
      imageTarget: "runtime",
      imageTag: "shado-cloud:deploy",
      smokePort: 9000,
   },
   {
      service: "shado-auth-api",
      contextSubdir: "shado-auth-api",
      dockerfile: "../Dockerfile.nestjs",
      imageTarget: "runtime",
      imageTag: "shado-auth-api:deploy",
      smokePort: 11001,
      // Boots AppModule, which connects to MySQL/Redis at startup — see the note above.
      smokeTest: false,
   },
   {
      service: "shado-metrics",
      contextSubdir: "shado-metrics",
      dockerfile: "../Dockerfile.nestjs",
      imageTarget: "runtime",
      imageTag: "shado-metrics:deploy",
      smokePort: 14001,
      smokeTest: false,
   },
   {
      service: "shado-music-api",
      contextSubdir: "shado-music-api",
      dockerfile: "../Dockerfile.shado-music-api",
      imageTarget: "runtime",
      imageTag: "shado-music-api:deploy",
      smokePort: 16001,
      smokeTest: false,
   },
   {
      service: "shado-gym-api",
      contextSubdir: "shado-gym-api",
      dockerfile: "../Dockerfile.nestjs",
      imageTarget: "runtime",
      imageTag: "shado-gym-api:deploy",
      smokePort: 15001,
      smokeTest: false,
   },
   // Frontends are `adapter-static` + Vite, so their VITE_* values are compiled INTO the bundle at
   // build time — there is nothing to mount at runtime. Their `.env` files are gitignored, so a
   // clean clone cannot build them; `envFile` supplies the values from this host for the duration
   // of the build. The runtime stage copies build output only, so they never reach an image layer.
   //
   // Served by nginx on port 80, which also answers /health with the same {"ok":true} shape the
   // APIs use, so the replica updater has one health contract to gate every swap on.
   {
      service: "shado-cloud-frontend",
      contextSubdir: "shado-cloud-frontend",
      dockerfile: "../Dockerfile.sveltekit",
      imageTarget: "runtime",
      imageTag: "shado-cloud-frontend:deploy",
      envFile: sibling("shado-cloud-frontend/.env"),
      smokePort: 80,
   },
   {
      service: "shado-music-frontend",
      contextSubdir: "shado-music-frontend",
      dockerfile: "../Dockerfile.sveltekit",
      imageTarget: "runtime",
      imageTag: "shado-music-frontend:deploy",
      envFile: sibling("shado-music-frontend/.env"),
      smokePort: 80,
   },
   {
      service: "shado-gym-app",
      contextSubdir: "shado-gym-app",
      dockerfile: "../Dockerfile.sveltekit",
      imageTarget: "runtime",
      imageTag: "shado-gym-app:deploy",
      envFile: sibling("shado-gym-app/.env"),
      smokePort: 80,
   },
];

/**
 * A NestJS API deployed on this host under pm2.
 *
 * `triggersRestart` is deliberately NOT set on the restart step: these are OTHER processes, so
 * restarting them doesn't kill the deployment that is driving them. Only the primary's own
 * project (`backend`) restarts the deployer itself. Setting it here would make `runSteps`
 * return early and silently drop every following step.
 */
function nestApiProject(opts: { slug: string; name: string; dir: string; pm2: string; hasTests?: boolean }): Partial<DeploymentProject> {
   const steps: DeploymentStepConfig[] = [
      { step: "git_pull", name: "Git Pull", cmd: "git", args: ["pull"] },
      { step: "npm_install", name: "NPM Install", cmd: "npm", args: ["install"] },
   ];
   if (opts.hasTests !== false) {
      steps.push({ step: "test", name: "Run Tests", cmd: "npm", args: ["test", "--", "--runInBand", "--no-colors"] });
   }
   steps.push(
      { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
      { step: "migrate", name: "Run Migrations", cmd: "npx", args: ["typeorm", "migration:run", "-d", "ormconfig.js"] },
      { step: "restart", name: "Restart Service", cmd: "pm2", args: ["restart", opts.pm2] },
      { step: "verify", name: "Verify Deployment", cmd: "pm2", args: ["describe", opts.pm2] },
   );
   return {
      slug: opts.slug,
      name: opts.name,
      workDir: sibling(opts.dir),
      pm2ProcessName: opts.pm2,
      branch: "master",
      steps: JSON.stringify(steps),
   };
}

/**
 * A SvelteKit frontend. All of them build to static output, so there is no process to restart —
 * the built assets are picked up by whatever serves them.
 */
function svelteFrontendProject(opts: { slug: string; name: string; dir: string }): Partial<DeploymentProject> {
   return {
      slug: opts.slug,
      name: opts.name,
      workDir: sibling(opts.dir),
      pm2ProcessName: null,
      branch: "master",
      steps: JSON.stringify([
         { step: "git_pull", name: "Git Pull", cmd: "git", args: ["pull"] },
         { step: "npm_install", name: "NPM Install", cmd: "npm", args: ["install"] },
         { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
      ] as DeploymentStepConfig[]),
   };
}

/**
 * Seeded on first boot for any slug not already in the table (see `seedDefaults`, which never
 * touches an existing row). pm2 process names follow the `shado-cloud-backend` convention
 * already in use; correct them in the admin UI if a host names them differently.
 */
const DEFAULT_PROJECTS: Partial<DeploymentProject>[] = [
   {
      // The primary itself — the one project whose restart kills the process running the
      // pipeline, which is why the steps after it resume via onModuleInit.
      slug: "backend",
      name: "Backend",
      workDir: "__CWD__",
      pm2ProcessName: "shado-cloud-backend",
      branch: "master",
      steps: JSON.stringify([
         { step: "git_pull", name: "Git Pull", cmd: "git", args: ["pull"] },
         { step: "npm_install", name: "NPM Install", cmd: "npm", args: ["install"] },
         { step: "test", name: "Run Tests", cmd: "npm", args: ["test", "--", "--runInBand", "--no-colors"] },
         { step: "build", name: "Build", cmd: "npm", args: ["run", "build"] },
         { step: "migrate", name: "Run Migrations", cmd: "npx", args: ["typeorm", "migration:run", "-d", "ormconfig.js"] },
         { step: "restart", name: "Restart Service", cmd: "pm2", args: ["restart", "shado-cloud-backend"], triggersRestart: true },
         { step: "verify", name: "Verify Deployment", cmd: "pm2", args: ["jlist"], runsOnModuleInit: true },
         // Get replicas onto this build: builds the image, proves it boots, then hands that one
         // image to every connected replica and reports each one's progress.
         //
         // Build and propagate are ONE step because they are one intent. As two, a pipeline could
         // hold the propagation half with no build to feed it, which presents as a step that spins
         // and then reports "no replicas" — with nothing in the UI to explain why.
         //
         // Built from a FRESH CLONE of the superproject, deleted afterwards, so the image contains
         // exactly what is committed on the branch: no config.yml (real secrets), no node_modules,
         // no dist/, no uncommitted local changes. It also means the paths below are relative to a
         // known layout rather than however this host is arranged.
         //
         // Last in the pipeline, so replicas only update once this node has restarted and verified
         // itself: a primary that fails its own restart never propagates a broken build outwards.
         {
            step: "propagate_replicas",
            name: "Propagate to Replicas",
            cmd: "",
            args: [],
            propagateToReplicas: true,
            buildImage: true,
            sourceRepo: SERVICES_REPO_SSH,
            sourceBranch: "main",
            // EVERY service, not just shado-cloud — a replica that can take over has to be running
            // the whole stack. All built from the one clone above.
            services: REPLICA_SERVICES,
         },
      ] as DeploymentStepConfig[]),
   },
   nestApiProject({ slug: "auth-api", name: "Auth API", dir: "shado-auth-api", pm2: "shado-auth-api" }),
   nestApiProject({ slug: "metrics", name: "Metrics", dir: "shado-metrics", pm2: "shado-metrics" }),
   nestApiProject({ slug: "music-api", name: "Music API", dir: "shado-music-api", pm2: "shado-music-api" }),
   // No `test` script in its package.json, so the test step would fail the pipeline outright.
   nestApiProject({ slug: "gym-api", name: "Gym API", dir: "shado-gym-api", pm2: "shado-gym-api", hasTests: false }),
   svelteFrontendProject({ slug: "frontend", name: "Frontend", dir: "shado-cloud-frontend" }),
   svelteFrontendProject({ slug: "music-frontend", name: "Music Frontend", dir: "shado-music-frontend" }),
   svelteFrontendProject({ slug: "gym-app", name: "Gym App", dir: "shado-gym-app" }),
];

@Injectable()
export class DeploymentService implements OnModuleInit {
   private deploymentSubject: Subject<MessageEvent> | null = null;
   private currentProcess: ReturnType<typeof spawn> | null = null;
   private cancelled = false;

   private readonly logger = new Logger(DeploymentService.name);

   constructor(
      private readonly config: ConfigService<EnvVariables>,
      @Inject() private readonly emailService: EmailService,
      @Inject() private readonly featureFlagService: FeatureFlagService,
      @Inject(REDIS_CACHE) private readonly redis: Redis,
      @InjectRepository(DeploymentProject) private readonly projectRepo: Repository<DeploymentProject>,
      @Inject() private readonly replicaPropagation: ReplicaPropagationService,
      @Inject() private readonly imageBuilder: ImageBuildService,
   ) {}

   private async sendDeploymentEmail(options: Parameters<EmailService["sendEmail"]>[0]) {
      try {
         if (await this.featureFlagService.isFeatureFlagEnabled(FeatureFlagNamespace.Admin, "allow_deployment_email_sending")) {
            await this.emailService.sendEmail(options);
         }
      } catch (e) {
         this.logger.error(`Failed to send deployment email: ${(e as Error).message}`);
      }
   }

   async onModuleInit() {
      await this.seedDefaults();

      // Resume any in-progress deployment after restart (non-blocking)
      const deployment = await this.getState(REDIS_KEY_CURRENT);
      if (deployment?.status === "running") {
         const project = await this.projectRepo.findOneBy({ slug: deployment.project });
         if (!project) return;
         const step = deployment.currentStep;
         this.logger.log(`Resuming deployment ${step.step} after restart...`);
         this.cancelled = false;
         // Open a fresh stream so the post-restart tail of the pipeline is observable: the
         // UI reconnects to /admin/deployment/stream once this process is back up. Without
         // this, emit() would go nowhere and steps after `restart` (e.g. "Propagate to
         // Replicas") would only be visible through status polling.
         this.deploymentSubject = new Subject<MessageEvent>();
         const remainingSteps = this.getFollowingSteps(step, project.getSteps());
         if (remainingSteps.length > 0) {
            deployment.currentStep = { step: remainingSteps[0].step, status: "running", output: "", startedAt: new Date() };
            await this.saveState(deployment, REDIS_KEY_CURRENT);
         }
         void this.runSteps(remainingSteps, this.resolveWorkDir(project), deployment.project, deployment);
      }
   }

   private async seedDefaults() {
      for (const def of DEFAULT_PROJECTS) {
         const exists = await this.projectRepo.findOneBy({ slug: def.slug });
         if (!exists) {
            const project = this.projectRepo.create(def);
            await this.projectRepo.save(project);
            // A fresh row already has every default step, so it starts at the current version.
            await this.redis.hset(REDIS_KEY_STEPS_VERSION, def.slug!, String(DEFAULT_STEPS_VERSION));
            this.logger.log(`Seeded deployment project: ${def.slug}`);
         } else {
            await this.reconcileDefaultSteps(exists, def);
         }
      }
   }

   /**
    * Bring an existing project's steps up to the current defaults, without disturbing anything the
    * operator has set.
    *
    * Strictly additive, in two ways:
    *   - a default step the project lacks is inserted at its position in the defaults
    *   - a default FIELD absent from an existing step is filled in
    *
    * Nothing is removed, reordered or overwritten, so command edits, skip flags and custom steps
    * survive. Field-level merging matters because a step can gain capabilities: the propagation
    * step grew image-build settings, and a project that already had a bare `propagate_replicas`
    * row would otherwise try to build with none of them configured.
    *
    * Guarded by a version recorded in Redis rather than re-running every boot, so a step or field
    * an operator deliberately clears afterwards stays cleared instead of reappearing.
    */
   private async reconcileDefaultSteps(project: DeploymentProject, def: Partial<DeploymentProject>): Promise<void> {
      const applied = Number((await this.redis.hget(REDIS_KEY_STEPS_VERSION, project.slug)) ?? 0);
      if (applied >= DEFAULT_STEPS_VERSION) return;

      let defaults: DeploymentStepConfig[];
      let current: DeploymentStepConfig[];
      try {
         defaults = JSON.parse(def.steps as string);
         current = project.getSteps();
      } catch (e) {
         this.logger.warn(`Skipping step reconciliation for ${project.slug}: ${(e as Error).message}`);
         return;
      }

      const changes: string[] = [];
      const merged = [...current];

      // 1. Repair values a previous version of this code seeded incorrectly.
      for (const fix of LEGACY_STEP_VALUE_FIXUPS) {
         const step = merged.find((s) => s.step === fix.step);
         if (step && (step as unknown as Record<string, unknown>)[fix.field] === fix.from) {
            (step as unknown as Record<string, unknown>)[fix.field] = fix.to;
            changes.push(`${fix.step}.${String(fix.field)}→${fix.to}`);
         }
      }

      // 2. Fill in fields a step is missing.
      for (const step of merged) {
         const defStep = defaults.find((d) => d.step === step.step);
         if (!defStep) continue;
         const added = Object.keys(defStep).filter((k) => !(k in step)) as (keyof DeploymentStepConfig)[];
         for (const key of added) {
            (step as unknown as Record<string, unknown>)[key] = (defStep as unknown as Record<string, unknown>)[key];
         }
         if (added.length > 0) changes.push(`${step.step}{${added.join(",")}}`);
      }

      // 3. Insert steps the project lacks entirely, after the last preceding default step it has,
      //    so relative order matches the defaults without assuming indexes.
      const have = new Set(merged.map((s) => s.step));
      for (const step of defaults.filter((s) => !have.has(s.step))) {
         const defIdx = defaults.findIndex((s) => s.step === step.step);
         const precedingIds = defaults.slice(0, defIdx).map((s) => s.step);
         let insertAt = merged.length;
         for (let i = merged.length - 1; i >= 0; i--) {
            if (precedingIds.includes(merged[i].step)) {
               insertAt = i + 1;
               break;
            }
         }
         merged.splice(insertAt, 0, { ...step });
         changes.push(`+${step.step}`);
      }

      if (changes.length > 0) {
         project.setSteps(merged);
         await this.projectRepo.save(project);
         this.logger.log(`Reconciled default steps for ${project.slug}: ${changes.join(", ")}`);
      }

      await this.redis.hset(REDIS_KEY_STEPS_VERSION, project.slug, String(DEFAULT_STEPS_VERSION));
   }

   /**
    * Expand a project's configured working directory.
    *
    * `__CWD__` is the primary's own checkout. It also works as a PREFIX, so sibling packages
    * can be addressed relatively (`__CWD__/../shado-auth-api`) and resolve correctly whether
    * the repos sit side by side in a dev workspace or on a deployment host. Anything else is
    * treated as an absolute path and returned untouched.
    */
   public resolveWorkDir(project: DeploymentProject): string {
      const dir = project.workDir;
      if (!dir) return "";
      if (dir === "__CWD__") return process.cwd();
      if (dir.startsWith("__CWD__/")) return path.resolve(process.cwd(), dir.slice("__CWD__/".length));
      return dir;
   }

   // --- State management ---

   private async saveState(deployment: DeploymentState | null, key: string) {
      if (deployment) {
         await this.redis.set(key, JSON.stringify(deployment), "EX", 86400);
      } else {
         await this.redis.del(key);
      }
   }

   private async getState(key: string): Promise<DeploymentState | null> {
      const data = await this.redis.get(key);
      if (!data) return null;
      const state = JSON.parse(data);
      state.completedSteps = state.completedSteps || {};
      return state;
   }

   // --- Public API ---

   public async getProjects(): Promise<DeploymentProject[]> {
      return this.projectRepo.find({ order: { id: "ASC" } });
   }

   public async getProject(slug: string): Promise<DeploymentProject | null> {
      return this.projectRepo.findOneBy({ slug });
   }

   public async saveProject(project: DeploymentProject): Promise<DeploymentProject> {
      return this.projectRepo.save(project);
   }

   public async deleteProject(slug: string): Promise<void> {
      await this.projectRepo.delete({ slug });
   }

   public async isRunning(): Promise<boolean> {
      const current = await this.getState(REDIS_KEY_CURRENT);
      return current?.status === "running";
   }

   public async getSteps(projectSlug: string): Promise<{ step: string; name: string; skip?: boolean; propagateToReplicas?: boolean; buildImage?: boolean; imageService?: string; services?: string[] }[]> {
      const project = await this.projectRepo.findOneBy({ slug: projectSlug });
      if (!project) return [];
      // cmd/args are deliberately withheld, but the step KIND has to be exposed: the UI renders
      // build and propagation steps differently, and cannot infer which is which from the name.
      return project.getSteps().map(s => ({
         step: s.step,
         name: s.name,
         skip: s.skip,
         propagateToReplicas: s.propagateToReplicas,
         buildImage: s.buildImage,
         imageService: s.imageService,
         // Names only. The UI lists what a propagation step will build — including before it runs,
         // when there are no staged images to infer the list from — but the specs themselves carry
         // host paths (envFile, smokeConfigFile) that an API response has no reason to disclose.
         services: this.serviceSpecs(s, projectSlug).map(spec => spec.service),
      }));
   }

   /** Replicas currently connected to the replica-link — what a propagation step would target. */
   public connectedReplicas() {
      return this.replicaPropagation.connectedReplicas();
   }

   public async getCurrentDeployment(): Promise<DeploymentState | null> {
      return this.getState(REDIS_KEY_CURRENT);
   }

   public async getLastDeployment(): Promise<DeploymentState | null> {
      return this.getState(REDIS_KEY_LAST);
   }

   public getSubject(): Subject<MessageEvent> | null {
      return this.deploymentSubject;
   }

   public async cancelDeployment(): Promise<void> {
      const current = await this.getState(REDIS_KEY_CURRENT);
      if (!current || current.status !== "running") {
         throw new Error("No deployment in progress");
      }
      this.cancelled = true;
      if (this.currentProcess) {
         this.currentProcess.kill("SIGTERM");
         this.currentProcess = null;
      }
      const runningStep = current.currentStep;
      if (runningStep) {
         runningStep.status = "failed";
         runningStep.error = "Cancelled by user";
         runningStep.finishedAt = new Date();
      }
      current.status = "failed";
      current.finishedAt = new Date();
      await this.saveState(current, REDIS_KEY_CURRENT);
      await this.saveState(current, REDIS_KEY_LAST);
      this.emit({ type: "deployment_complete", deployment: current });
      this.deploymentSubject?.complete();
      this.logger.log("Deployment cancelled by user");
   }

   public async enqueue(projectSlug: string, triggeredBy: string): Promise<void> {
      await this.redis.rpush(REDIS_KEY_QUEUE, JSON.stringify({ projectSlug, triggeredBy }));
      this.logger.log(`Queued deployment for ${projectSlug} (triggered by ${triggeredBy})`);
   }

   public async getQueue(): Promise<{ projectSlug: string; triggeredBy: string }[]> {
      const items = await this.redis.lrange(REDIS_KEY_QUEUE, 0, -1);
      return items.map(i => JSON.parse(i));
   }

   private async processQueue(): Promise<void> {
      const next = await this.redis.lpop(REDIS_KEY_QUEUE);
      if (!next) return;
      const { projectSlug, triggeredBy } = JSON.parse(next);
      this.logger.log(`Processing queued deployment for ${projectSlug}`);
      try {
         await this.startDeployment(projectSlug, triggeredBy);
      } catch (e) {
         this.logger.error(`Queued deployment failed to start: ${(e as Error).message}`);
      }
   }

   public async retryStep(step: string): Promise<Subject<MessageEvent>> {
      if (await this.isRunning()) {
         throw new Error("Deployment already in progress");
      }
      const current = await this.getState(REDIS_KEY_CURRENT);
      if (!current || current.status !== "failed") {
         throw new Error("No failed deployment to retry");
      }
      const stepState = current.currentStep;
      if (!stepState || stepState.status !== "failed") {
         throw new Error("Step not found or not failed");
      }

      current.currentStep = { status: "pending", output: "", error: undefined, startedAt: undefined, finishedAt: undefined, step };
      current.status = "running";
      current.finishedAt = undefined;
      await this.saveState(current, REDIS_KEY_CURRENT);

      this.deploymentSubject = new Subject<MessageEvent>();
      this.cancelled = false;

      const project = await this.projectRepo.findOneBy({ slug: current.project });
      if (!project) throw new Error(`Project ${current.project} not found`);

      const stepsToRun = this.getFollowingSteps(current.currentStep, project.getSteps());
      void this.runSteps(stepsToRun, this.resolveWorkDir(project), current.project, current);
      return this.deploymentSubject;
   }

   public async startDeployment(projectSlug: string, triggeredBy: string): Promise<Subject<MessageEvent>> {
      if (await this.isRunning()) {
         throw new Error("Deployment already in progress");
      }

      const project = await this.projectRepo.findOneBy({ slug: projectSlug });
      if (!project) throw new Error(`Project "${projectSlug}" not found`);
      if (!project.enabled) throw new Error(`Project "${projectSlug}" is disabled`);

      const steps = project.getSteps();
      const workDir = this.resolveWorkDir(project);
      if (!workDir) {
         throw new Error(`Working directory not configured for project "${projectSlug}"`);
      }

      this.deploymentSubject = new Subject<MessageEvent>();
      this.cancelled = false;
      const deployment: DeploymentState = {
         id: `deploy_${Date.now()}`,
         project: projectSlug,
         status: "running",
         currentStep: {
            step: steps[0].step,
            status: "pending",
            output: "",
            startedAt: undefined,
            finishedAt: undefined,
            error: "",
            attempt: 1,
            maxAttempts: 3,
         },
         completedSteps: {},
         startedAt: new Date(),
         triggeredBy,
      };
      await this.saveState(deployment, REDIS_KEY_CURRENT);

      void this.runDeployment(steps, workDir, projectSlug, deployment);

      return this.deploymentSubject;
   }

   /**
    * The project's propagation step, or null if it has none.
    *
    * Only one makes sense per pipeline, so the first is the answer.
    */
   public async getPropagationStep(projectSlug: string): Promise<DeploymentStepConfig | null> {
      const project = await this.projectRepo.findOneBy({ slug: projectSlug });
      if (!project) return null;
      try {
         return project.getSteps().find((s) => s.propagateToReplicas) ?? null;
      } catch {
         return null;
      }
   }

   /**
    * Images from the most recent run whose artifacts are STILL on disk, and can therefore be
    * handed to a replica without rebuilding.
    *
    * Filtered rather than reported from the deployment state alone: artifacts are pruned on a TTL
    * and live in the OS temp dir, so a recorded image is not evidence the bytes survive. Offering
    * a re-send that cannot work would fail on the replica mid-deployment instead of here.
    */
   public async availableStagedImages(): Promise<ReplicaImageRef[]> {
      const state = (await this.getState(REDIS_KEY_CURRENT)) ?? (await this.getState(REDIS_KEY_LAST));
      return (state?.images ?? []).filter((i) => this.imageBuilder.hasStagedArtifact(i.artifact));
   }

   /**
    * Run the propagation step ON ITS OWN, without the pipeline in front of it.
    *
    * This exists because a propagation with no replicas online is a SUCCESS, not a failure — the
    * step logs "nothing to propagate to" and passes through, which is correct (a replica-less
    * install must not fail every deployment). But it also means `retryStep` cannot help: it
    * requires a failed deployment and a failed step. So a replica that happened to be offline
    * during the deployment had no way back onto the current build short of redeploying the
    * primary — restarting a healthy production process to fix a remote node that was merely
    * asleep.
    *
    * `reuseImage` re-sends an image an earlier run already built and staged, skipping the clone,
    * build and smoke test. That is the common case for this button: the build was fine, the
    * replicas simply were not there to receive it. Verified present first — see
    * {@link availableStagedImages} — and refused rather than silently falling back to a source
    * deployment, which would order the replica down a completely different path.
    */
   public async startPropagation(
      projectSlug: string,
      triggeredBy: string,
      opts: { reuseImage?: boolean } = {},
   ): Promise<Subject<MessageEvent>> {
      if (await this.isRunning()) {
         throw new Error("Deployment already in progress");
      }

      const project = await this.projectRepo.findOneBy({ slug: projectSlug });
      if (!project) throw new Error(`Project "${projectSlug}" not found`);
      if (!project.enabled) throw new Error(`Project "${projectSlug}" is disabled`);

      const configured = project.getSteps().find((s) => s.propagateToReplicas);
      if (!configured) {
         throw new Error(`Project "${projectSlug}" has no "Propagate to Replicas" step to run`);
      }

      const workDir = this.resolveWorkDir(project);
      if (!workDir) {
         throw new Error(`Working directory not configured for project "${projectSlug}"`);
      }

      // `skip` is cleared: an operator pressing the button is an explicit instruction, and
      // honouring the flag here would produce a button that reports success having done nothing.
      const stepConfig: DeploymentStepConfig = { ...configured, skip: false };

      let images: ReplicaImageRef[] | undefined;
      if (opts.reuseImage) {
         images = await this.availableStagedImages();
         if (images.length === 0) {
            throw new Error(
               "No image from a previous run is still staged on this host — propagate with a rebuild instead",
            );
         }
         stepConfig.buildImage = false;
      }

      this.deploymentSubject = new Subject<MessageEvent>();
      this.cancelled = false;
      const deployment: DeploymentState = {
         id: `propagate_${Date.now()}`,
         project: projectSlug,
         status: "running",
         currentStep: {
            step: stepConfig.step,
            status: "pending",
            output: "",
            attempt: 1,
            maxAttempts: 1,
         },
         completedSteps: {},
         startedAt: new Date(),
         triggeredBy,
         images,
      };
      await this.saveState(deployment, REDIS_KEY_CURRENT);

      this.logger.log(
         `Manual replica propagation for ${projectSlug} (triggered by ${triggeredBy})` +
            `${opts.reuseImage ? ` — re-sending ${images!.length} staged image(s)` : " — rebuilding the image"}`,
      );

      // Through runDeployment, so the feature-flag gate, notifications and queue hand-off behave
      // exactly as they do for a full pipeline. A one-step pipeline is still a deployment.
      void this.runDeployment([stepConfig], workDir, projectSlug, deployment);

      return this.deploymentSubject;
   }

   // --- Internal ---

   private getFollowingSteps(step: StepState, allSteps: DeploymentStepConfig[]) {
      const idx = allSteps.findIndex(s => s.step === step.step);
      // If the current step already succeeded (e.g. restart), start from the next one
      if (step.status === "success" && idx >= 0) {
         return allSteps.slice(idx + 1);
      }
      return allSteps.slice(idx);
   }

   private async runSteps(
      steps: DeploymentStepConfig[],
      workDir: string,
      projectSlug: string,
      deployment: DeploymentState,
   ) {
      const frontendUrl = this.config.get("this-service.frontend_url", { infer: true }) || "";
      const deployPageUrl = `${frontendUrl}/admin/deploy`;

      for (const stepConfig of steps) {
         if (this.cancelled) return;

         const stepState = deployment.currentStep;
         stepState.step = stepConfig.step;
         stepState.output = "";
         stepState.error = undefined;
         stepState.propagation = undefined;

         if (stepConfig.skip) {
            stepState.status = "skipped";
            stepState.output = "Skipped (permanently disabled)\n";
            deployment.completedSteps[stepConfig.step] = { ...stepState };
            await this.saveState(deployment, REDIS_KEY_CURRENT);
            this.emit({ type: "step_complete", step: stepConfig.step, status: "skipped" });
            continue;
         }

         // A standalone image-build step from before build and propagate were merged. It carries no
         // command, so falling through would run an empty shell. Skip it with an explanation rather
         // than deleting it from the operator's pipeline behind their back.
         if (stepConfig.buildImage && !stepConfig.propagateToReplicas) {
            stepState.status = "skipped";
            stepState.output =
               "Skipped: building the replica image is now part of the \"Propagate to Replicas\" step.\n" +
               "This step does nothing and can be deleted from the pipeline.\n";
            deployment.completedSteps[stepConfig.step] = { ...stepState };
            await this.saveState(deployment, REDIS_KEY_CURRENT);
            this.emit({ type: "step_output", step: stepConfig.step, output: stepState.output });
            this.emit({ type: "step_complete", step: stepConfig.step, status: "skipped" });
            continue;
         }

         // Fan the deployment out to every connected replica instead of running a command. Builds
         // and stages the image first unless the step opts out.
         if (stepConfig.propagateToReplicas) {
            const failed = await this.runPropagateStep(stepConfig, projectSlug, workDir, deployment);
            if (failed) {
               await this.failDeployment(deployment, stepConfig, projectSlug, deployPageUrl);
               return;
            }
            continue;
         }

         // Handle restart step — triggers process restart, remaining steps resume on init
         if (stepConfig.triggersRestart) {
            stepState.status = "running";
            stepState.startedAt = new Date();
            stepState.output = `Initiating restart via: ${stepConfig.cmd} ${stepConfig.args.join(" ")}...\n`;
            await this.saveState(deployment, REDIS_KEY_CURRENT);
            this.emit({ type: "step_start", step: stepConfig.step, startedAt: stepState.startedAt });
            this.emit({ type: "step_output", step: stepConfig.step, output: stepState.output });

            stepState.status = "success";
            stepState.finishedAt = new Date();
            stepState.output += "Restart command sent. Remaining steps will run after restart.\n";
            deployment.completedSteps[stepConfig.step] = { ...stepState };
            await this.saveState(deployment, REDIS_KEY_CURRENT);
            this.emit({ type: "step_output", step: stepConfig.step, output: "Restart command sent. Remaining steps will run after restart.\n" });
            this.emit({ type: "step_complete", step: stepConfig.step, status: "success", finishedAt: stepState.finishedAt });

            const proc = spawn(stepConfig.cmd, stepConfig.args, { detached: true, stdio: "ignore", shell: true });
            if (proc.unref) proc.unref();
            return;
         }

         const maxAttempts = 3;
         stepState.attempt = 1;
         stepState.maxAttempts = maxAttempts;

         while (stepState.attempt <= maxAttempts && !this.cancelled) {
            stepState.status = "running";
            stepState.startedAt = new Date();
            stepState.error = undefined;
            if (stepState.attempt > 1) {
               stepState.output += `\n--- Retry attempt ${stepState.attempt}/${maxAttempts} ---\n`;
               this.emit({ type: "step_output", step: stepConfig.step, output: `\n--- Retry attempt ${stepState.attempt}/${maxAttempts} ---\n` });
            }
            await this.saveState(deployment, REDIS_KEY_CURRENT);
            this.emit({ type: "step_start", step: stepConfig.step, startedAt: stepState.startedAt, attempt: stepState.attempt, maxAttempts });

            try {
               await this.runStep(stepConfig.cmd, stepConfig.args, workDir, stepConfig.step, deployment);
               stepState.status = "success";
               stepState.finishedAt = new Date();
               deployment.completedSteps[stepConfig.step] = { ...stepState };
               await this.saveState(deployment, REDIS_KEY_CURRENT);
               this.emit({ type: "step_complete", step: stepConfig.step, status: "success", finishedAt: stepState.finishedAt });
               break;
            } catch (error) {
               stepState.error = (error as Error).message;
               if (stepState.attempt < maxAttempts) {
                  stepState.output += `\nAttempt ${stepState.attempt} failed: ${stepState.error}\n`;
                  this.emit({ type: "step_output", step: stepConfig.step, output: `\nAttempt ${stepState.attempt} failed: ${stepState.error}\n` });
                  stepState.attempt++;
                  await new Promise(r => setTimeout(r, 2000));
               } else {
                  stepState.status = "failed";
                  stepState.finishedAt = new Date();
                  deployment.completedSteps[stepConfig.step] = { ...stepState };
                  this.emit({ type: "step_complete", step: stepConfig.step, status: "failed", error: stepState.error, finishedAt: stepState.finishedAt });
                  break;
               }
            }
         }

         if (stepState.status === "failed") {
            await this.failDeployment(deployment, stepConfig, projectSlug, deployPageUrl);
            return;
         }
      }

      deployment.status = "success";
      deployment.finishedAt = new Date();
      await this.saveState(deployment, REDIS_KEY_CURRENT);
      await this.saveState(deployment, REDIS_KEY_LAST);
      this.emit({ type: "deployment_complete", deployment });
      this.deploymentSubject?.complete();
      this.logger.log(`Deployment completed successfully`);

      const duration = Math.round((new Date(deployment.finishedAt).getTime() - new Date(deployment.startedAt).getTime()) / 1000);
      void this.sendDeploymentEmail({
         subject: `Shado Cloud - ${projectSlug} deployment SUCCESS`,
         html: this.buildEmailHtml({
            title: "Deployment Successful",
            status: "success",
            project: projectSlug,
            triggeredBy: deployment.triggeredBy,
            duration: `${duration}s`,
            deployPageUrl,
         }),
      });
      void this.processQueue();
   }

   /**
    * Terminal failure path shared by command steps and the propagation step: persist the
    * failed state, close the SSE stream, notify, and hand off to the queue.
    */
   private async failDeployment(
      deployment: DeploymentState,
      stepConfig: DeploymentStepConfig,
      projectSlug: string,
      deployPageUrl: string,
   ): Promise<void> {
      deployment.status = "failed";
      deployment.finishedAt = new Date();
      await this.saveState(deployment, REDIS_KEY_CURRENT);
      await this.saveState(deployment, REDIS_KEY_LAST);
      this.emit({ type: "deployment_complete", deployment });
      this.deploymentSubject?.complete();
      this.logger.error(`Deployment failed at ${stepConfig.step}: ${deployment.currentStep.error}`);
      void this.sendDeploymentEmail({
         subject: `Shado Cloud - ${projectSlug} deployment FAILED`,
         html: this.buildEmailHtml({
            title: "Deployment Failed",
            status: "failed",
            project: projectSlug,
            triggeredBy: deployment.triggeredBy,
            failedStep: stepConfig.name,
            error: deployment.currentStep.error,
            deployPageUrl,
         }),
      });
      void this.processQueue();
   }

   /**
    * Order every connected replica to deploy itself and collect their status + logs.
    *
    * Returns true if the step FAILED. By default replica failures are reported but don't sink
    * the primary's pipeline (the primary is already built and about to restart); set
    * `requireAllReplicas` on the step to make any replica failure fatal.
    */
   private async runPropagateStep(
      stepConfig: DeploymentStepConfig,
      projectSlug: string,
      workDir: string,
      deployment: DeploymentState,
   ): Promise<boolean> {
      const stepState = deployment.currentStep;
      stepState.status = "running";
      stepState.startedAt = new Date();
      stepState.attempt = 1;
      stepState.maxAttempts = 1;
      await this.saveState(deployment, REDIS_KEY_CURRENT);
      this.emit({ type: "step_start", step: stepConfig.step, startedAt: stepState.startedAt });

      const appendLog = (line: string) => {
         stepState.output += line;
         this.emit({ type: "step_output", step: stepConfig.step, output: line });
      };

      // Set when building from a throwaway clone, so it is removed however this step ends.
      let disposeClone: (() => void) | undefined;

      try {
         const project = await this.projectRepo.findOneBy({ slug: projectSlug });

         // Build the image replicas will run, then hand that one image to all of them. One step
         // rather than two: a separate build step could be missing from a pipeline, leaving the
         // propagation half with nothing to send — which presents as a step that spins and then
         // reports "no replicas".
         if (stepConfig.buildImage !== false) {
            const built = await this.buildReplicaImages(stepConfig, projectSlug, workDir, project?.branch, appendLog);
            disposeClone = built.disposeClone;
            // Replace by service rather than appending, so re-running the step does not leave a
            // stale entry for a service alongside its rebuild.
            const rebuilt = new Set(built.images.map((i) => i.service));
            deployment.images = [
               ...(deployment.images ?? []).filter((i) => !rebuilt.has(i.service)),
               ...built.images,
            ];
            await this.saveState(deployment, REDIS_KEY_CURRENT);
            this.emit({ type: "replica_image_staged", step: stepConfig.step, images: deployment.images });
         } else if (deployment.images?.length) {
            // Re-sending what an earlier run staged. Distinguished from the no-image case below
            // because the two order replicas down completely different paths — an image swap
            // applied by the updater, versus each replica building from source — and reporting
            // the wrong one would send an operator looking in the wrong place.
            appendLog(`Re-sending ${deployment.images.length} image(s) staged by an earlier run; no rebuild.\n`);
            for (const img of deployment.images) {
               appendLog(`  - ${img.service}: ${img.imageId.slice(0, 19)}… (${Math.round(img.size / 1024 / 1024)}MB)\n`);
            }
            appendLog("\n");
         } else {
            appendLog("Image build skipped — replicas will be ordered to deploy themselves from source.\n\n");
         }

         // Replica log deltas are streamed, not persisted per chunk. Persist on a throttle too,
         // so a client that is polling (or reconnecting after the primary's own restart, when
         // the SSE stream was lost) still sees the logs advance rather than jumping at step
         // boundaries.
         let lastSave = 0;
         const throttledSave = () => {
            const nowMs = Date.now();
            if (nowMs - lastSave < 2000) return;
            lastSave = nowMs;
            void this.saveState(deployment, REDIS_KEY_CURRENT);
         };

         const propagation = await this.replicaPropagation.propagate(
            {
               deploymentId: deployment.id,
               project: projectSlug,
               branch: project?.branch,
               triggeredBy: deployment.triggeredBy,
               timeoutMs: stepConfig.propagateTimeoutMs,
               waitForReplicasMs: stepConfig.propagateWaitForReplicasMs,
               // When a buildImage step ran earlier, replicas replace these images instead of
               // building from source.
               images: deployment.images,
            },
            {
               onLog: appendLog,
               // `propagation` is mutated in place by the service, so holding the reference
               // keeps the persisted snapshot live without re-assigning on every update.
               onDispatch: (initial) => {
                  stepState.propagation = initial;
                  this.emit({ type: "replica_dispatch", step: stepConfig.step, propagation: initial });
                  void this.saveState(deployment, REDIS_KEY_CURRENT);
               },
               onReplicaUpdate: (replica) => {
                  this.emit({ type: "replica_update", step: stepConfig.step, replica });
                  // Keep the persisted snapshot current so a reload mid-propagation still
                  // renders the per-replica timeline.
                  void this.saveState(deployment, REDIS_KEY_CURRENT);
               },
               onReplicaOutput: (replicaId, output) => {
                  this.emit({ type: "replica_output", step: stepConfig.step, replicaId, output });
                  throttledSave();
               },
            },
         );

         stepState.propagation = propagation;
         const summary = this.replicaPropagation.summarize(propagation);
         const allOk = this.replicaPropagation.allSucceeded(propagation);

         this.emit({ type: "replica_done", step: stepConfig.step, propagation });

         if (!allOk && stepConfig.requireAllReplicas) {
            stepState.status = "failed";
            stepState.error = `Replica propagation incomplete: ${summary}`;
            stepState.finishedAt = new Date();
            deployment.completedSteps[stepConfig.step] = { ...stepState };
            await this.saveState(deployment, REDIS_KEY_CURRENT);
            this.emit({ type: "step_complete", step: stepConfig.step, status: "failed", error: stepState.error, finishedAt: stepState.finishedAt });
            return true;
         }

         if (!allOk) appendLog(`Continuing: replica failures are not fatal for this step (${summary}).\n`);

         stepState.status = "success";
         stepState.finishedAt = new Date();
         deployment.completedSteps[stepConfig.step] = { ...stepState };
         await this.saveState(deployment, REDIS_KEY_CURRENT);
         this.emit({ type: "step_complete", step: stepConfig.step, status: "success", finishedAt: stepState.finishedAt });
         return false;
      } catch (e) {
         stepState.status = "failed";
         stepState.error = (e as Error).message;
         stepState.finishedAt = new Date();
         deployment.completedSteps[stepConfig.step] = { ...stepState };
         await this.saveState(deployment, REDIS_KEY_CURRENT);
         this.emit({ type: "step_complete", step: stepConfig.step, status: "failed", error: stepState.error, finishedAt: stepState.finishedAt });
         return true;
      } finally {
         if (disposeClone) {
            appendLog("Removing the build clone.\n");
            disposeClone();
         }
      }
   }

   /**
    * The services this step builds, as an explicit list.
    *
    * A step configured before multi-service support has no `services` array, so its legacy
    * single-image fields are read as a one-element list. That keeps an existing pipeline behaving
    * exactly as it did, which matters because these are edited by hand in the admin UI and a
    * silent change of meaning would be invisible until a replica ran the wrong thing.
    */
   private serviceSpecs(stepConfig: DeploymentStepConfig, projectSlug: string): ReplicaServiceBuildSpec[] {
      if (stepConfig.services?.length) return stepConfig.services;
      return [
         {
            service: stepConfig.imageService ?? projectSlug,
            contextSubdir: stepConfig.contextSubdir,
            dockerfile: stepConfig.dockerfile,
            imageTarget: stepConfig.imageTarget,
            imageTag: stepConfig.imageTag,
            smokePort: stepConfig.smokePort,
            smokeConfigFile: stepConfig.smokeConfigFile,
            smokeTest: stepConfig.smokeTest,
         },
      ];
   }

   /**
    * Expand a `__CWD__`-prefixed path the same way `resolveWorkDir` does.
    *
    * Applies to `envFile` and `smokeConfigFile`, which point at files on THIS host rather than
    * inside the clone — a frontend's `.env` and a service's config are exactly the things a
    * repository must not carry, so they are addressed relative to the primary's checkout.
    */
   private resolveHostPath(p: string): string {
      if (p === "__CWD__") return process.cwd();
      if (p.startsWith("__CWD__/")) return path.resolve(process.cwd(), p.slice("__CWD__/".length));
      return p;
   }

   /**
    * Build, prove and stage an image for EVERY service the step configures, from one shared clone.
    *
    * All services, not just shado-cloud, because a replica that can take over has to be running
    * all of them — a mirror needs the file tree, a failover node needs the stack. The delivery
    * protocol already carried an array keyed by compose service and the replica's updater already
    * loops over it, so this is the build half catching up.
    *
    * One clone shared across every build: each service is a submodule of the superproject, so
    * cloning once and naming all the needed submodules costs one fetch instead of N, and guarantees
    * every image in a deployment comes from the same moment in the branch's history.
    *
    * FAILS FAST. A service that will not build or will not boot sinks the whole step before
    * anything is dispatched, because a half-applied set is worse than none: the replica would be
    * left running a mix of old and new services with no record of which. The smoke test is what
    * makes this meaningful — the primary runs under pm2 and never from an image, so these images
    * are otherwise never exercised until a replica tries to start them.
    */
   private async buildReplicaImages(
      stepConfig: DeploymentStepConfig,
      projectSlug: string,
      workDir: string,
      projectBranch: string | undefined,
      appendLog: (chunk: string) => void,
   ): Promise<{ images: ReplicaImageRef[]; disposeClone?: () => void }> {
      const specs = this.serviceSpecs(stepConfig, projectSlug);
      let disposeClone: (() => void) | undefined;

      // Build from a fresh clone when configured: the images then reflect exactly what is
      // committed, rather than this host's working trees — which hold real config.yml files,
      // node_modules, dist/ and possibly uncommitted changes.
      let cloneDir = workDir;
      if (stepConfig.sourceRepo) {
         const branch = stepConfig.sourceBranch ?? projectBranch ?? "master";
         // Every context subdir is a submodule, and NOTHING in this project advances the
         // superproject's gitlinks — so each one must be checked out at its branch tip or the
         // image is built from whatever commit the pointer was parked on.
         const submodules = [...new Set(specs.map((s) => s.contextSubdir).filter((s): s is string => !!s))];
         const clone = await this.imageBuilder.cloneSource(stepConfig.sourceRepo, branch, appendLog, submodules);
         disposeClone = clone.dispose;
         cloneDir = clone.dir;
      }

      const images: ReplicaImageRef[] = [];
      try {
         for (const [i, spec] of specs.entries()) {
            appendLog(`\n── ${spec.service} (${i + 1}/${specs.length}) ──\n`);
            const buildDir = spec.contextSubdir ? path.join(cloneDir, spec.contextSubdir) : cloneDir;

            // Frontends bake their config into the bundle, so the values have to be present for
            // the build. Removed immediately afterwards, whatever happens.
            const disposeEnv = spec.envFile
               ? this.imageBuilder.stageEnvFile(this.resolveHostPath(spec.envFile), buildDir, appendLog)
               : undefined;

            try {
               const tag = spec.imageTag ?? `${spec.service}:deploy`;
               const imageId = await this.imageBuilder.build(
                  { workDir: buildDir, dockerfile: spec.dockerfile, target: spec.imageTarget, tag },
                  appendLog,
               );

               if (spec.smokeTest !== false) {
                  await this.imageBuilder.smokeTest(
                     {
                        imageId,
                        workDir: buildDir,
                        configFile: spec.smokeConfigFile ? this.resolveHostPath(spec.smokeConfigFile) : undefined,
                        port: spec.smokePort ?? 9000,
                        path: spec.smokePath,
                     },
                     appendLog,
                  );
               } else {
                  appendLog(`Smoke test skipped for ${spec.service} by step configuration.\n`);
               }

               const artifact = await this.imageBuilder.stage(imageId, appendLog);
               images.push({
                  service: spec.service,
                  imageId: artifact.imageId,
                  tarSha256: artifact.tarSha256,
                  size: artifact.size,
                  artifact: artifact.artifact,
                  // The replica's updater gates the swap on this, so it is deliberately the SAME
                  // target the smoke test just probed — one definition, verified here and re-checked
                  // there, rather than a second copy on the replica that can drift out of step.
                  healthPort: spec.smokePort ?? 9000,
                  healthPath: spec.smokePath,
               });
            } catch (e) {
               // Name the service in the failure. With one image the step WAS the service, so a
               // bare "docker build exited with code 1" was unambiguous; across eight builds it
               // says nothing about which one to go and look at.
               throw new Error(`${spec.service}: ${(e as Error).message}`);
            } finally {
               disposeEnv?.();
            }
         }

         appendLog(`\nBuilt and staged ${images.length} image(s): ${images.map((i) => i.service).join(", ")}\n\n`);
         return { images, disposeClone };
      } catch (e) {
         // The caller only disposes a clone it was handed back, so clean up on the failure path.
         disposeClone?.();
         throw e;
      }
   }

   private async runDeployment(
      steps: DeploymentStepConfig[],
      workDir: string,
      projectSlug: string,
      deployment: DeploymentState,
   ) {
      const frontendUrl = this.config.get("this-service.frontend_url", { infer: true }) || "";
      const deployPageUrl = `${frontendUrl}/admin/deploy`;

      if (await this.featureFlagService.isFeatureFlagDisabled(FeatureFlagNamespace.Admin, "enable_pipeline_deployment")) {
         this.logger.warn("Deployment blocked: enable_pipeline_deployment feature flag is disabled");
         deployment.status = "failed";
         deployment.currentStep.status = "failed";
         deployment.currentStep.error = "Deployments are disabled (feature flag: enable_pipeline_deployment)";
         await this.saveState(deployment, REDIS_KEY_CURRENT);
         await this.saveState(deployment, REDIS_KEY_LAST);
         this.emit({ type: "deployment_complete", deployment });
         this.deploymentSubject?.complete();
         return;
      }

      void this.sendDeploymentEmail({
         subject: `Shado Cloud - ${projectSlug} deployment started`,
         html: this.buildEmailHtml({
            title: "Deployment Started",
            status: "running",
            project: projectSlug,
            triggeredBy: deployment.triggeredBy,
            deployPageUrl,
         }),
      });

      await this.runSteps(steps, workDir, projectSlug, deployment);
   }

   private async runStep(cmd: string, args: string[], cwd: string, step: string, deployment: DeploymentState): Promise<void> {
      const env = {
         PATH: process.env.PATH,
         HOME: process.env.HOME,
         SHELL: process.env.SHELL,
         FORCE_COLOR: "0",
         NO_COLOR: "1",
         PM2_NO_INTERACTION: "1",
         CI: "true",
      };
      const stepState = deployment.currentStep;
      const stripAnsi = (str: string) => str.replace(/\x1B\[[0-9;]*[a-zA-Z]/g, "");

      const cwdLine = `[cwd: ${cwd}] $ ${cmd} ${args.join(" ")}\n`;
      stepState.output += cwdLine;
      this.emit({ type: "step_output", step, output: cwdLine });

      const actualCwd = await new Promise<string>((res) => {
         const p = spawn("pwd", [], { cwd, shell: true, stdio: ["ignore", "pipe", "pipe"] });
         let out = "";
         p.stdout.on("data", (d) => out += d.toString());
         p.on("close", () => res(out.trim()));
      });
      const pwdLine = `[pwd: ${actualCwd}]\n`;
      stepState.output += pwdLine;
      this.emit({ type: "step_output", step, output: pwdLine });

      return new Promise((resolve, reject) => {
         const proc = spawn(cmd, args, { cwd, shell: true, env, stdio: ["ignore", "pipe", "pipe"] });
         this.currentProcess = proc;

         proc.stdout.on("data", (data) => {
            const output = stripAnsi(data.toString());
            stepState.output += output;
            this.emit({ type: "step_output", step, output });
         });

         proc.stderr.on("data", (data) => {
            const output = stripAnsi(data.toString());
            stepState.output += output;
            this.emit({ type: "step_output", step, output });
         });

         proc.on("close", (code) => {
            this.currentProcess = null;
            if (code === 0 || code === null) resolve();
            else reject(new Error(`Process exited with code ${code}`));
         });

         proc.on("error", (err) => {
            this.currentProcess = null;
            reject(err);
         });
      });
   }

   private emit(event: DeploymentEvent) {
      if (this.deploymentSubject) {
         this.deploymentSubject.next({ data: JSON.stringify(event) } as MessageEvent);
      }
   }

   private buildEmailHtml(opts: {
      title: string;
      status: "running" | "success" | "failed";
      project: string;
      triggeredBy: string;
      deployPageUrl: string;
      failedStep?: string;
      error?: string;
      duration?: string;
   }): string {
      const statusColors = {
         running: { bg: "#3b82f6", text: "In Progress" },
         success: { bg: "#22c55e", text: "Success" },
         failed: { bg: "#ef4444", text: "Failed" },
      };
      const status = statusColors[opts.status];

      return `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f7; padding: 40px 20px; margin: 0;">
   <div style="max-width: 500px; margin: 0 auto; background: white; border-radius: 16px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1);">
      <div style="background: ${status.bg}; padding: 24px; text-align: center;">
         <h1 style="color: white; margin: 0; font-size: 24px;">${opts.title}</h1>
      </div>
      <div style="padding: 24px;">
         <table style="width: 100%; border-collapse: collapse;">
            <tr>
               <td style="padding: 8px 0; color: #666;">Project</td>
               <td style="padding: 8px 0; text-align: right; font-weight: 600;">${opts.project}</td>
            </tr>
            <tr>
               <td style="padding: 8px 0; color: #666;">Status</td>
               <td style="padding: 8px 0; text-align: right;">
                  <span style="background: ${status.bg}; color: white; padding: 4px 12px; border-radius: 12px; font-size: 12px;">${status.text}</span>
               </td>
            </tr>
            <tr>
               <td style="padding: 8px 0; color: #666;">Triggered by</td>
               <td style="padding: 8px 0; text-align: right;">${opts.triggeredBy}</td>
            </tr>
            ${opts.duration ? `<tr><td style="padding: 8px 0; color: #666;">Duration</td><td style="padding: 8px 0; text-align: right;">${opts.duration}</td></tr>` : ""}
            ${opts.failedStep ? `<tr><td style="padding: 8px 0; color: #666;">Failed at</td><td style="padding: 8px 0; text-align: right; color: #ef4444;">${opts.failedStep}</td></tr>` : ""}
         </table>
         ${opts.error ? `<div style="margin-top: 16px; padding: 12px; background: #fef2f2; border-radius: 8px; font-family: monospace; font-size: 12px; color: #991b1b; word-break: break-all;">${opts.error}</div>` : ""}
         <a href="${opts.deployPageUrl}" style="display: block; margin-top: 24px; padding: 12px; background: #1f2937; color: white; text-align: center; text-decoration: none; border-radius: 8px; font-weight: 500;">View Deployment</a>
      </div>
   </div>
</body>
</html>`;
   }
}
