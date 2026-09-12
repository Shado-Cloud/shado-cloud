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
    * If true, this step ignores `cmd`/`args` entirely and instead orders every replica
    * connected to the replica-link to deploy itself, streaming each replica's step status
    * and log output back to the primary. Place it BEFORE any `triggersRestart` step so the
    * primary is still alive to collect the results.
    */
   propagateToReplicas?: boolean;
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
    * If true, this step ignores `cmd`/`args` and instead builds a container image, smoke-tests
    * it, and stages it for replicas to download.
    *
    * A later `propagateToReplicas` step then hands replicas the resulting image instead of
    * ordering them to build from source. The primary itself does not run from the image, so
    * the smoke test is what stops a broken build reaching a host you cannot reach.
    */
   buildImage?: boolean;
   /**
    * Build only. Git URL to clone into a temp directory and build from, deleted afterwards.
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
   /** Build only. Branch to clone. Defaults to the project's `branch`. */
   sourceBranch?: string;
   /**
    * Build only. Subdirectory of the clone to use as the build context, e.g. `shado-cloud` when
    * cloning the services superproject. Omit to use the clone root.
    */
   contextSubdir?: string;
   /** Build only. Dockerfile path relative to the build context. */
   dockerfile?: string;
   /** Build only. Multi-stage target. Defaults to `runtime`. */
   imageTarget?: string;
   /** Build only. Local tag for the built image. Defaults to `<slug>:deploy`. */
   imageTag?: string;
   /**
    * Build only. Compose service name the image belongs to on a replica — this is what the
    * replica's updater matches against to know which container to recreate.
    */
   imageService?: string;
   /**
    * Build only. Absolute path to a config file mounted at /app/config.yml during the smoke
    * test. Without one the image must be able to boot from its own defaults.
    */
   smokeConfigFile?: string;
   /** Build only. Container port the smoke test probes /health on. Defaults to 9000. */
   smokePort?: number;
   /** Build only. Set false to stage the image without smoke-testing it first. */
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
