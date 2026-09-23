import { BaseEntity, Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, UpdateDateColumn } from "typeorm";

/**
 * One service image a propagation step builds and hands to replicas.
 *
 * A replica that is only a mirror needs shado-cloud alone. A replica that can take over needs
 * every service, so a propagation step builds a LIST of these — one per compose service on the
 * replica. They share a single clone of the superproject, since each service is a submodule of it.
 *
 * The delivery protocol already carried an array (`ReplicaImageRef[]`, keyed by compose service),
 * and the replica's updater already loops over it and tags per service. Only the build side was
 * single-image.
 */
export interface ReplicaServiceBuildSpec {
   /**
    * Compose service name on the REPLICA — this is what the replica's updater matches against to
    * know which container to recreate, and what its per-service health gate resolves. It must
    * match a service in the replica's docker-compose.yml or the deployment has nowhere to land.
    */
   service: string;
   /**
    * Subdirectory of the clone to use as the build context, e.g. `shado-auth-api`. Also the
    * submodule checked out at its branch tip. Omit to use the clone root.
    */
   contextSubdir?: string;
   /** Dockerfile path relative to the build context. */
   dockerfile?: string;
   /** Multi-stage target. Defaults to `runtime`. */
   imageTarget?: string;
   /** Local tag for the built image. Defaults to `<service>:deploy`. */
   imageTag?: string;
   /** Container port the smoke test probes. Defaults to 9000. */
   smokePort?: number;
   /** Path the smoke test probes. Defaults to `/health`. */
   smokePath?: string;
   /**
    * Absolute path ON THE PRIMARY to a config file mounted at /app/config.yml for the smoke test.
    * Supports the `__CWD__` prefix. When omitted a throwaway placeholder is generated.
    */
   smokeConfigFile?: string;
   /**
    * Absolute path ON THE PRIMARY to an env file copied into the build context before building.
    * Supports the `__CWD__` prefix.
    *
    * This is what makes a frontend buildable from a clean clone at all. SvelteKit frontends are
    * `adapter-static` + Vite, so `VITE_*` values are compiled INTO the bundle at build time — and
    * their `.env` files are gitignored, so a fresh clone contains none of them and would produce a
    * bundle pointing at nothing. The file is copied in for the build and removed afterwards, so it
    * never reaches an image layer.
    */
   envFile?: string;
   /** Set false to stage without smoke-testing first. */
   smokeTest?: boolean;
}

export interface DeploymentStepConfig {
   step: string;
   name: string;
   cmd: string;
   args: string[];
   /** If true, this step triggers a process restart — remaining steps resume on module init */
   triggersRestart?: boolean;
   /** If true, this step runs after module init (post-restart verification) */
   runsOnModuleInit?: boolean;
   /** If true, this step is permanently skipped during deployment */
   skip?: boolean;
   /**
    * Propagation only. When true the step fails unless every replica deployed successfully.
    * Default (false) reports replica failures without failing the primary's pipeline.
    */
   requireAllReplicas?: boolean;
   /** Propagation only. How long a replica may take before it's marked timed out (ms). */
   propagateTimeoutMs?: number;
   /**
    * Propagation only. How long to wait for replicas to (re)connect before treating the step
    * as a no-op (ms). Relevant when this step runs after a `triggersRestart` step, since the
    * restart drops every replica-link socket and replicas re-dial on their own delay.
    */
   propagateWaitForReplicasMs?: number;
   /**
    * If true, this step ignores `cmd`/`args` and instead orders every replica connected to the
    * replica-link to update, streaming each one's status and log output back.
    *
    * Unless `buildImage` is explicitly false, it first builds the container image replicas will
    * run, smoke-tests it, and stages it — then hands that one image to every replica. Build and
    * propagate are one step because they are one intent, and splitting them meant a pipeline
    * could hold the propagation half with no build to feed it, which presents as a step that
    * spins and then reports "no replicas".
    */
   propagateToReplicas?: boolean;
   /**
    * Propagation only. Build and stage an image before dispatching. Defaults to true.
    *
    * Set false to fall back to ordering replicas to deploy themselves from source, for a replica
    * that has no updater container.
    */
   buildImage?: boolean;
   /**
    * Propagation only. Git URL to clone into a temp directory and build from, deleted afterwards.
    *
    * Preferred over building from the primary's live checkout: a fresh clone contains exactly
    * what is committed — no `config.yml` (which holds real secrets), no `node_modules`, no
    * `dist/`, no uncommitted changes — so the image is reproducible from the branch alone. It
    * also makes the layout inside the build context known, rather than depending on how this
    * host happens to be arranged.
    *
    * Omit to build from the project's own working directory instead.
    */
   sourceRepo?: string;
   /** Propagation only. Branch to clone. Defaults to the project's `branch`. */
   sourceBranch?: string;
   /**
    * Propagation only. Every service image to build and deliver, one per compose service on the
    * replica. Built from one shared clone of `sourceRepo`.
    *
    * This is what lets a replica act as a FAILOVER node rather than just a file mirror: it has to
    * be running every service, not only shado-cloud. The delivery protocol already carried an
    * array and the replica's updater already loops over it — only the build side was single-image.
    *
    * When absent, the legacy single-image fields below are used as a one-element list, so a
    * pipeline configured before this existed keeps behaving exactly as it did.
    */
   services?: ReplicaServiceBuildSpec[];
   /**
    * Propagation only. Subdirectory of the clone to use as the build context, e.g. `shado-cloud`
    * when cloning the services superproject. Omit to use the clone root.
    *
    * Legacy single-image field — superseded by `services`, and ignored when that is set.
    */
   contextSubdir?: string;
   /** Propagation only. Dockerfile path relative to the build context. Superseded by `services`. */
   dockerfile?: string;
   /** Propagation only. Multi-stage target. Defaults to `runtime`. Superseded by `services`. */
   imageTarget?: string;
   /** Propagation only. Local tag for the built image. Defaults to `<slug>:deploy`. Superseded by `services`. */
   imageTag?: string;
   /**
    * Propagation only. Compose service name the image belongs to on a replica — this is what the
    * replica's updater matches against to know which container to recreate.
    *
    * Superseded by `services`.
    */
   imageService?: string;
   /**
    * Propagation only. Absolute path to a config file mounted at /app/config.yml during the smoke
    * test. When omitted a throwaway placeholder is generated. Superseded by `services`.
    */
   smokeConfigFile?: string;
   /** Propagation only. Container port the smoke test probes /health on. Defaults to 9000. Superseded by `services`. */
   smokePort?: number;
   /** Propagation only. Set false to stage the image without smoke-testing it first. */
   smokeTest?: boolean;
}

@Entity()
export class DeploymentProject extends BaseEntity {
   @PrimaryGeneratedColumn()
   id: number;

   /** Unique slug e.g. "backend", "frontend" */
   @Column({ unique: true })
   slug: string;

   @Column()
   name: string;

   /** Absolute path to the project working directory */
   @Column()
   workDir: string;

   /** PM2 process name (if applicable, used for restart step) */
   @Column({ nullable: true })
   pm2ProcessName: string | null;

   /** JSON-serialized DeploymentStepConfig[] */
   @Column({ type: "text" })
   steps: string;

   /** Git branch to watch for webhook deployments */
   @Column({ default: "master" })
   branch: string;

   @Column({ default: true })
   enabled: boolean;

   @CreateDateColumn()
   created_at: Date;

   @UpdateDateColumn()
   updated_at: Date;

   getSteps(): DeploymentStepConfig[] {
      return JSON.parse(this.steps);
   }

   setSteps(steps: DeploymentStepConfig[]) {
      this.steps = JSON.stringify(steps);
   }
}
