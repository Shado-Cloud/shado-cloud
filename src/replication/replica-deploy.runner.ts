import { Injectable, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { execFileSync, spawn, type ChildProcess } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { EnvVariables } from "src/config/config.validator";
import type { ReplicaDeployAck, ReplicaDeployProgress, ReplicaDeployRequest, ReplicaDeployStepInfo } from "./replica-link.constants";

/** A single command the replica runs when it deploys itself. */
export interface ReplicaDeployStep {
   step: string;
   name: string;
   cmd: string;
   args: string[];
   /** Last step of the pipeline: restarts this process, so nothing after it can be reported. */
   triggersRestart?: boolean;
}

/** True when this process is running inside a container (Docker/Podman/Kubernetes). */
function isContainerized(): boolean {
   if (process.env.KUBERNETES_SERVICE_HOST) return true;
   try {
      return fs.existsSync("/.dockerenv") || fs.existsSync("/run/.containerenv");
   } catch {
      return false;
   }
}

/**
 * The pipeline a replica runs when the master orders it to deploy. Fixed by design — see
 * {@link ReplicaDeployRunner.pipeline}.
 *
 * Deliberately does NOT run migrations: a replica's database is overwritten wholesale by the
 * master's dump on the replication cron, so migrating locally is pointless and can fight that
 * restore.
 *
 * Containerized replicas get neither a build nor a restart step. The image runs the app under
 * `nest start --watch` (see docker-entrypoint.sh), and `dist/` is gitignored — so `git pull`
 * lands new `src/**` files, tsc recompiles them, and the watcher restarts the process. The
 * pull IS the restart trigger, which is why no pm2 is needed in the container.
 *
 * Running `npm run build` there would be worse than redundant: `nest build` writes the same
 * `dist/` and the same incremental `.tsbuildinfo` that the live watcher owns, while the node
 * process is executing out of that directory — two compilers, one output dir.
 */
function defaultPipeline(containerized: boolean): ReplicaDeployStep[] {
   const steps: ReplicaDeployStep[] = [
      { step: "git_pull", name: "Git Pull", cmd: "git", args: ["pull"] },
      { step: "npm_install", name: "NPM Install", cmd: "npm", args: ["install"] },
   ];
   if (!containerized) {
      steps.push({ step: "build", name: "Build", cmd: "npm", args: ["run", "build"] });
      // Same process name the primary uses: a replica runs the same codebase under pm2.
      steps.push({ step: "restart", name: "Restart Service", cmd: "pm2", args: ["restart", "shado-cloud-backend"], triggersRestart: true });
   }
   return steps;
}

/** How long log output is buffered before being flushed to the master, in ms. */
const OUTPUT_FLUSH_MS = 150;
/** Flush early once the buffer reaches this many characters. */
const OUTPUT_FLUSH_CHARS = 8 * 1024;
/** Grace period between the last progress frame and actually restarting, so the frame lands. */
const RESTART_GRACE_MS = 750;

type Emit = (progress: ReplicaDeployProgress) => void;

/**
 * Replica-side deployment executor. The master can order a deployment over the replica-link
 * (see DEPLOY_EVENT), and this runs the replica's OWN pipeline — the order carries no
 * commands — streaming step transitions and log output back as it goes.
 *
 * Only ever active on a node whose replication role is `replica`; the master never receives
 * a DEPLOY_EVENT because nothing dials into it on that channel.
 */
@Injectable()
export class ReplicaDeployRunner {
   private readonly logger = new Logger(ReplicaDeployRunner.name);

   /** Non-null while a deployment is in flight — one at a time. */
   private activeRunId: string | null = null;
   private currentProcess: ChildProcess | null = null;

   constructor(private readonly config: ConfigService<EnvVariables>) {}

   /**
    * The directory the replica deploys in: its own working directory. There is nothing to
    * configure — a replica deploys the checkout it is running from.
    */
   public workDir(): string {
      return process.cwd();
   }

   /**
    * The pipeline this replica runs when ordered to deploy. Fixed, not configurable: the order
    * from the master carries no commands (so the replica is never a remote-exec target), and a
    * replica cannot hold its own configuration in the database either — its schema is dropped
    * and re-imported wholesale from the master's dump on the replication cron.
    */
   public pipeline(): ReplicaDeployStep[] {
      return defaultPipeline(isContainerized());
   }

   public isBusy(): boolean {
      return this.activeRunId !== null;
   }

   public steps(): ReplicaDeployStepInfo[] {
      return this.pipeline().map((s) => ({ step: s.step, name: s.name }));
   }

   /**
    * Checks that the pipeline can actually run here, so an environment problem is reported to
    * the master as an explicit rejection ("git is not installed") rather than surfacing later
    * as an opaque `Process exited with code 127` on step 1.
    *
    * Containerized replicas are the reason this exists: the image has to ship git, the repo
    * bind-mount has to be a real working copy, and private-repo credentials have to be injected
    * at runtime — any of which can be missing on a given host.
    */
   private preflight(pipeline: ReplicaDeployStep[], workDir: string): string | null {
      const needsGit = pipeline.some((s) => s.cmd === "git" || s.cmd.endsWith("/git"));
      if (!needsGit) return null;

      try {
         execFileSync("git", ["--version"], { stdio: "ignore" });
      } catch {
         return isContainerized()
            ? "git is not installed in this replica's container image — add it (e.g. `apk add git`) and rebuild"
            : "git is not installed on this replica";
      }

      if (!fs.existsSync(path.join(workDir, ".git"))) {
         return `${workDir} is not a git working copy on this replica — check the deploy work-dir${isContainerized() ? " and that the repo (including .git) is mounted into the container" : ""}`;
      }

      try {
         execFileSync("git", ["rev-parse", "--is-inside-work-tree"], { cwd: workDir, stdio: "ignore" });
      } catch {
         return `git refuses to operate in ${workDir} on this replica — a bind-mounted repo usually needs \`git config --global --add safe.directory ${workDir}\``;
      }

      return null;
   }

   /**
    * Accept-or-refuse a deployment order from the master. Returns the ack synchronously; the
    * run itself proceeds in the background and reports through `emit`.
    */
   public accept(req: ReplicaDeployRequest, emit: Emit): ReplicaDeployAck {
      if (this.isBusy()) {
         return { accepted: false, reason: `Already deploying (run ${this.activeRunId})` };
      }
      const pipeline = this.pipeline();
      const workDir = this.workDir();
      if (!workDir) {
         return { accepted: false, reason: "No working directory resolved on this replica" };
      }

      const problem = this.preflight(pipeline, workDir);
      if (problem) {
         this.logger.warn(`Refusing deployment order ${req.runId}: ${problem}`);
         return { accepted: false, reason: problem };
      }

      this.activeRunId = req.runId;
      this.logger.log(`Accepted deployment order ${req.runId} from master (project "${req.project}") — ${pipeline.length} step(s) in ${workDir}`);
      void this.run(req, pipeline, workDir, emit);

      return { accepted: true, workDir, steps: this.steps() };
   }

   private async run(req: ReplicaDeployRequest, pipeline: ReplicaDeployStep[], workDir: string, emit: Emit): Promise<void> {
      const frame = (partial: Omit<ReplicaDeployProgress, "runId" | "at">): void =>
         emit({ ...partial, runId: req.runId, at: Date.now() });

      try {
         frame({
            phase: "step_output",
            step: pipeline[0].step,
            output:
               `[replica deploy ${req.runId}]\n` +
               `[ordered by master — project "${req.project}"${req.branch ? `, branch ${req.branch}` : ""}` +
               `${req.triggeredBy ? `, triggered by ${req.triggeredBy}` : ""}]\n`,
         });

         for (const step of pipeline) {
            frame({ phase: "step_start", step: step.step });

            // A restart step kills this process, so report it as done BEFORE spawning it.
            // The master confirms the outcome by waiting for the replica to reconnect.
            if (step.triggersRestart) {
               frame({
                  phase: "step_output",
                  step: step.step,
                  output: `[cwd: ${workDir}] $ ${step.cmd} ${step.args.join(" ")}\n` + "Restart command sent; this replica will drop off the link and reconnect.\n",
               });
               frame({ phase: "step_complete", step: step.step, status: "success" });
               frame({ phase: "finished", ok: true, restarting: true });

               setTimeout(() => {
                  const proc = spawn(step.cmd, step.args, { cwd: workDir, detached: true, stdio: "ignore", shell: true });
                  if (proc.unref) proc.unref();
               }, RESTART_GRACE_MS);
               return;
            }

            try {
               await this.runCommand(step, workDir, (output) => frame({ phase: "step_output", step: step.step, output }));
               frame({ phase: "step_complete", step: step.step, status: "success" });
            } catch (e) {
               const error = (e as Error).message;
               frame({ phase: "step_complete", step: step.step, status: "failed", error });
               frame({ phase: "finished", ok: false, error: `${step.name}: ${error}` });
               this.logger.error(`Deployment order ${req.runId} failed at ${step.step}: ${error}`);
               return;
            }
         }

         frame({ phase: "finished", ok: true });
         this.logger.log(`Deployment order ${req.runId} completed successfully`);
      } catch (e) {
         frame({ phase: "finished", ok: false, error: (e as Error).message });
         this.logger.error(`Deployment order ${req.runId} aborted: ${(e as Error).message}`);
      } finally {
         this.activeRunId = null;
         this.currentProcess = null;
      }
   }

   private runCommand(step: ReplicaDeployStep, cwd: string, onOutput: (chunk: string) => void): Promise<void> {
      const env = {
         PATH: process.env.PATH,
         HOME: process.env.HOME,
         SHELL: process.env.SHELL,
         FORCE_COLOR: "0",
         NO_COLOR: "1",
         PM2_NO_INTERACTION: "1",
         CI: "true",
      };
      const stripAnsi = (str: string): string => str.replace(/\x1B\[[0-9;]*[a-zA-Z]/g, "");

      // Coalesce chunks so a chatty build doesn't turn into thousands of socket frames.
      let buffer = "";
      let flushTimer: NodeJS.Timeout | null = null;
      const flush = (): void => {
         if (flushTimer) {
            clearTimeout(flushTimer);
            flushTimer = null;
         }
         if (buffer) {
            onOutput(buffer);
            buffer = "";
         }
      };
      const push = (chunk: string): void => {
         buffer += chunk;
         if (buffer.length >= OUTPUT_FLUSH_CHARS) flush();
         else if (!flushTimer) flushTimer = setTimeout(flush, OUTPUT_FLUSH_MS);
      };

      push(`[cwd: ${cwd}] $ ${step.cmd} ${step.args.join(" ")}\n`);

      return new Promise<void>((resolve, reject) => {
         const proc = spawn(step.cmd, step.args, { cwd, shell: true, env, stdio: ["ignore", "pipe", "pipe"] });
         this.currentProcess = proc;

         proc.stdout?.on("data", (data: Buffer) => push(stripAnsi(data.toString())));
         proc.stderr?.on("data", (data: Buffer) => push(stripAnsi(data.toString())));

         proc.on("close", (code) => {
            this.currentProcess = null;
            flush();
            if (code === 0 || code === null) resolve();
            else reject(new Error(`Process exited with code ${code}`));
         });

         proc.on("error", (err) => {
            this.currentProcess = null;
            flush();
            reject(err);
         });
      });
   }
}
