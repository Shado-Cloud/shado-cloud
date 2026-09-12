import { BaseEntity, Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, UpdateDateColumn } from "typeorm";

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
    * Propagation only. Subdirectory of the clone to use as the build context, e.g. `shado-cloud`
    * when cloning the services superproject. Omit to use the clone root.
    */
   contextSubdir?: string;
   /** Propagation only. Dockerfile path relative to the build context. */
   dockerfile?: string;
   /** Propagation only. Multi-stage target. Defaults to `runtime`. */
   imageTarget?: string;
   /** Propagation only. Local tag for the built image. Defaults to `<slug>:deploy`. */
   imageTag?: string;
   /**
    * Propagation only. Compose service name the image belongs to on a replica — this is what the
    * replica's updater matches against to know which container to recreate.
    */
   imageService?: string;
   /**
    * Propagation only. Absolute path to a config file mounted at /app/config.yml during the smoke
    * test. When omitted a throwaway placeholder is generated.
    */
   smokeConfigFile?: string;
   /** Propagation only. Container port the smoke test probes /health on. Defaults to 9000. */
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
