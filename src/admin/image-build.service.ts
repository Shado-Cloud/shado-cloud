import { Injectable, Logger } from "@nestjs/common";
import { spawn } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { ImageArtifactService, type ImageArtifact } from "src/replication/image-artifact.service";

export interface ImageBuildOptions {
   /** Directory containing the build context (the service's checkout). */
   workDir: string;
   /** Dockerfile path, relative to workDir. Defaults to `Dockerfile`. */
   dockerfile?: string;
   /** Multi-stage target. Defaults to `runtime` — the slim, self-contained stage. */
   target?: string;
   /** Local tag applied to the build, e.g. `shado-cloud:deploy`. */
   tag: string;
}

export interface SmokeTestOptions {
   imageId: string;
   /** Build context directory — a throwaway config is written here when none is supplied. */
   workDir: string;
   /**
    * Path to a config file to mount at /app/config.yml. When omitted a placeholder replica
    * config is generated, because the image cannot boot without one.
    */
   configFile?: string;
   /** Port inside the container to probe. */
   port: number;
   /** How long to wait for /health to answer before declaring the image bad. */
   timeoutMs?: number;
}

const SMOKE_TIMEOUT_MS = 90_000;
const SMOKE_POLL_MS = 2_000;
/** Throwaway config written into the build context for the duration of a smoke test. */
const SMOKE_CONFIG_FILE = ".smoke-config.yml";
/** Prefix for throwaway build clones, so strays are identifiable in the temp dir. */
const CLONE_PREFIX = "shado-build-";

/**
 * Builds container images on the primary and proves they work before any replica is told to run
 * them.
 *
 * The smoke test is the point of this service. The primary does not run from an image — it runs
 * node directly under pm2 — so an image built here is otherwise NEVER exercised until a replica
 * tries to start it. A broken image would then surface as a remote host that fails to come back
 * up. Starting it locally and probing /health moves that discovery to the machine an operator is
 * already sitting at.
 */
@Injectable()
export class ImageBuildService {
   private readonly logger = new Logger(ImageBuildService.name);

   constructor(private readonly artifacts: ImageArtifactService) {}

   /** Whether a docker CLI is usable here at all. Reported as a clear step failure if not. */
   public async isDockerAvailable(): Promise<boolean> {
      try {
         await this.run("docker", ["version", "--format", "{{.Server.Version}}"], process.cwd());
         return true;
      } catch {
         return false;
      }
   }

   /**
    * Clone a repository into a temp directory for a clean build, returning the path and a
    * disposer that removes it.
    *
    * Shallow, with shallow submodules: the build needs the current tree, not history, and a full
    * clone of a nine-submodule superproject would move far more than necessary on every deploy.
    *
    * The caller MUST call `dispose()` in a finally — a few hundred MB per deployment would
    * otherwise accumulate in the temp directory indefinitely.
    */
   public async cloneSource(
      repo: string,
      branch: string,
      onLog: (chunk: string) => void,
   ): Promise<{ dir: string; dispose: () => void }> {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), CLONE_PREFIX));
      const dispose = (): void => {
         try {
            fs.rmSync(dir, { recursive: true, force: true });
         } catch (e) {
            this.logger.warn(`Could not remove build clone ${dir}: ${(e as Error).message}`);
         }
      };

      try {
         onLog(`Cloning ${repo} (${branch}) into ${dir}...\n`);
         await this.run(
            "git",
            [
               // Every URL in .gitmodules is HTTPS, so --recurse-submodules would try to clone
               // each submodule over HTTPS even when the superproject URL is SSH — and fail asking
               // for a username. Rewriting at clone time keeps .gitmodules untouched (it is shared
               // with everyone else's checkouts) while sending every fetch over SSH. `-c` travels
               // to the submodule clones because git passes it down via GIT_CONFIG_PARAMETERS.
               "-c", "url.git@github.com:.insteadOf=https://github.com/",
               "clone", "--depth", "1", "--branch", branch,
               "--recurse-submodules", "--shallow-submodules",
               repo, dir,
            ],
            os.tmpdir(),
            onLog,
         );

         const head = (await this.run("git", ["rev-parse", "--short", "HEAD"], dir)).trim();
         onLog(`Cloned at ${head}.\n`);
         return { dir, dispose };
      } catch (e) {
         // Never leave a partial clone behind on failure.
         dispose();
         throw new Error(`Could not clone ${repo} (${branch}): ${(e as Error).message}${this.cloneHint(repo)}`);
      }
   }

   /**
    * Turn an authentication failure into something actionable. The pipeline runs unattended under
    * a process manager, so the usual signals (a prompt, an interactive retry) are absent and the
    * raw git error is misleading — an HTTPS clone with no credentials reports
    * "could not read Username ... No such device or address", which describes the missing TTY
    * rather than the missing credential.
    */
   private cloneHint(repo: string): string {
      if (repo.startsWith("git@") || repo.startsWith("ssh://")) {
         return (
            "\n\nSSH clone failed. Check, as the user this service runs as" +
            ` (currently ${os.userInfo().username}, HOME=${os.homedir()}):` +
            "\n  - a usable key exists in ~/.ssh (a key set up for your login user is NOT visible" +
            " to a different service user)" +
            "\n  - `ssh -T git@github.com` succeeds for that user" +
            "\n  - github.com is in ~/.ssh/known_hosts, or the key has no passphrase / an agent is" +
            " reachable via SSH_AUTH_SOCK" +
            `\n  - SSH_AUTH_SOCK is ${process.env.SSH_AUTH_SOCK ? "set" : "NOT set in this process"}`
         );
      }
      return (
         "\n\nHTTPS clone failed, which needs a username and token — SSH keys do not apply to an" +
         " https:// URL. Either switch the step's source repo to git@github.com:… or configure a" +
         " credential helper for the user this service runs as."
      );
   }

   /** Build an image and return its image ID. */
   public async build(opts: ImageBuildOptions, onLog: (chunk: string) => void): Promise<string> {
      if (!(await this.isDockerAvailable())) {
         throw new Error(
            "Docker is not available on this host. Replica images are built here even though this node does not run containers — install Docker or skip this step.",
         );
      }

      const args = [
         "build",
         "-f", opts.dockerfile ?? "Dockerfile",
         "--target", opts.target ?? "runtime",
         "-t", opts.tag,
         ".",
      ];
      onLog(`[cwd: ${opts.workDir}] $ docker ${args.join(" ")}\n`);
      await this.run("docker", args, opts.workDir, onLog);

      const imageId = (await this.run("docker", ["image", "inspect", opts.tag, "--format", "{{.Id}}"], opts.workDir)).trim();
      if (!imageId) throw new Error(`Could not resolve an image ID for ${opts.tag} after build`);

      onLog(`Built ${opts.tag} → ${imageId}\n`);
      this.logger.log(`Built image ${opts.tag} (${imageId})`);
      return imageId;
   }

   /**
    * Start the image, wait for /health, tear it down. Throws if it never becomes healthy.
    *
    * Deliberately runs with no database or Redis reachable: /health does not touch them, so this
    * verifies the image boots on its own merits rather than testing the primary's dependencies.
    */
   public async smokeTest(opts: SmokeTestOptions, onLog: (chunk: string) => void): Promise<void> {
      const name = `shado-smoke-${Date.now()}`;
      const timeoutMs = opts.timeoutMs ?? SMOKE_TIMEOUT_MS;

      // config.loader readFileSync's config.yml with no existence check, so the container cannot
      // boot without one. Synthesise a throwaway when the step doesn't name a file — the
      // alternative, mounting the primary's own config, would both put real secrets in a
      // throwaway container and boot AppModule (which needs a database) instead of the
      // ReplicationModule a replica actually runs.
      const { configFile, cleanupConfig } = opts.configFile
         ? { configFile: path.resolve(opts.configFile), cleanupConfig: () => undefined }
         : this.writeSmokeConfig(opts.workDir, opts.port);

      // `-p 0:<port>` rather than `-P`: the latter only publishes ports declared with EXPOSE, and
      // the runtime image declares none (the port is configurable, so baking one in would be a
      // guess). Host port 0 asks Docker for an ephemeral one, which avoids colliding with the
      // primary's own listener on the same box.
      const runArgs = ["run", "-d", "--name", name, "-p", `0:${opts.port}`, "-e", `HEALTHCHECK_PORT=${opts.port}`];
      runArgs.push("-v", `${configFile}:/app/config.yml:ro`);
      runArgs.push(opts.imageId);

      onLog(`Smoke testing ${opts.imageId}...\n`);
      await this.run("docker", runArgs, process.cwd(), onLog);

      try {
         // Ask docker which host port it chose.
         const mapping = (await this.run("docker", ["port", name, `${opts.port}/tcp`], process.cwd())).trim();
         const hostPort = mapping.split("\n")[0]?.split(":").pop();
         if (!hostPort) throw new Error(`Container did not publish port ${opts.port}`);

         const deadline = Date.now() + timeoutMs;
         let lastError = "";
         while (Date.now() < deadline) {
            const state = (await this.run("docker", ["inspect", name, "--format", "{{.State.Status}}"], process.cwd()).catch(() => "")).trim();
            if (state === "exited" || state === "dead") {
               const logs = await this.runCombined("docker", ["logs", "--tail", "40", name], process.cwd());
               throw new Error(`Container exited during smoke test. Last output:\n${logs}`);
            }

            try {
               const res = await fetch(`http://127.0.0.1:${hostPort}/health`, { signal: AbortSignal.timeout(4000) });
               if (res.ok) {
                  const body = await res.text();
                  onLog(`Smoke test passed: ${body}\n`);
                  return;
               }
               lastError = `HTTP ${res.status}`;
            } catch (e) {
               lastError = (e as Error).message;
            }
            await new Promise((r) => setTimeout(r, SMOKE_POLL_MS));
         }

         const logs = await this.runCombined("docker", ["logs", "--tail", "40", name], process.cwd());
         throw new Error(`Image did not become healthy within ${Math.round(timeoutMs / 1000)}s (${lastError}). Last output:\n${logs}`);
      } finally {
         // Always clean up, including on failure — a stuck smoke container would block the next
         // deploy and leak a port.
         await this.run("docker", ["rm", "-f", name], process.cwd()).catch(() => undefined);
         cleanupConfig();
      }
   }

   /**
    * Write a throwaway replica config for the smoke test, next to the build context.
    *
    * In the build context rather than the OS temp dir on purpose: Docker Desktop on macOS does
    * not share /tmp or /var/folders, and a bind mount from there silently becomes an empty
    * DIRECTORY, which surfaces as a baffling EISDIR crash inside the container. The build
    * context is mountable by definition — Docker just built from it.
    *
    * `role: replica` so the image boots ReplicationModule, which needs no database. Values are
    * placeholders: nothing here connects anywhere, and /health touches no dependency.
    */
   private writeSmokeConfig(workDir: string, port: number): { configFile: string; cleanupConfig: () => void } {
      const configFile = path.join(workDir, SMOKE_CONFIG_FILE);
      const config = [
         "this-service:",
         "  stage: dev",
         "  host: localhost",
         "  port:",
         `    http: ${port}`,
         `    tcp: ${port + 1}`,
         // Must EXIST in the container: cloud-dir is validated with ValidFilePath, and a
         // non-existent path fails config validation before the app can boot.
         "  cloud-dir: /tmp",
         "  password-vault-salt: smoke-test-placeholder",
         "  replication:",
         "    role: replica",
         "    master-or-replica-ip: 127.0.0.1",
         "    ignore-patterns: []",
         "    mirror-dirs: []",
         "    replicate-database: false",
         "  frontend_url: http://localhost:3000",
         "  google:",
         "    email: smoke@example.com",
         "    client-id: smoke",
         "    client-secret: smoke",
         "    refresh-token: smoke",
         "  deployment:",
         "    github-webhook-secret: smoke",
         "  cold-storage:",
         "    drives: []",
         "cross-service:",
         "  secret: smoke-test-placeholder",
         "  auth-api:",
         "    host: 127.0.0.1",
         "    port:",
         "      http: 11001",
         "      tcp: 11002",
         "  metrics-api:",
         "    host: 127.0.0.1",
         "    port:",
         "      http: 14001",
         "      tcp: 14002",
         "db:",
         "  type: mysql",
         "  host: 127.0.0.1",
         "  port: 3306",
         "  username: root",
         "  password: smoke",
         "  name: shado_cloud",
         "redis:",
         "  host: 127.0.0.1",
         "  port: 6379",
         "  password: smoke",
         "",
      ].join("\n");

      fs.writeFileSync(configFile, config, "utf-8");
      return {
         configFile,
         cleanupConfig: () => {
            try {
               if (fs.existsSync(configFile)) fs.unlinkSync(configFile);
            } catch { /* best-effort */ }
         },
      };
   }

   /** `docker save` the image and stage it for replicas to download. */
   public async stage(imageId: string, onLog: (chunk: string) => void): Promise<ImageArtifact> {
      return this.artifacts.export(imageId, onLog);
   }

   /**
    * Run a command, resolving with its stdout. `onLog` receives stdout and stderr as they arrive
    * so a long build streams into the deployment log rather than appearing all at once.
    */
   /** Combined stdout+stderr, for `docker logs` — a crashing container reports on stderr. */
   private async runCombined(cmd: string, args: string[], cwd: string): Promise<string> {
      try {
         const { stdout, stderr } = await this.runBoth(cmd, args, cwd);
         return `${stdout}${stderr}`.trim() || "(no output)";
      } catch {
         return "(logs unavailable)";
      }
   }

   private async run(cmd: string, args: string[], cwd: string, onLog?: (chunk: string) => void): Promise<string> {
      return (await this.runBoth(cmd, args, cwd, onLog)).stdout;
   }

   /**
    * Minimal environment for spawned git/docker, deliberately not the full process env.
    *
    * The SSH bits matter: HOME alone finds on-disk keys and ~/.ssh/config, but a passphrase-
    * protected key needs the agent, and the agent is only reachable through SSH_AUTH_SOCK. Losing
    * that variable turns a working SSH setup into an authentication failure with no obvious cause.
    *
    * GIT_TERMINAL_PROMPT=0 makes a missing credential say so. Without it git tries to prompt, and
    * because this runs unattended with no TTY the failure surfaces as
    * "could not read Username ... No such device or address" — which describes the absent terminal
    * rather than the absent credential, and sends you looking in the wrong place.
    *
    * StrictHostKeyChecking=accept-new so a host never seen before is trusted on first contact
    * instead of blocking on a confirmation nobody can answer. It still refuses a CHANGED host key,
    * which is the case that actually signals interception.
    */
   private childEnv(): NodeJS.ProcessEnv {
      return {
         PATH: process.env.PATH,
         HOME: process.env.HOME,
         DOCKER_BUILDKIT: "0",
         // Never prompt; fail with a readable error instead.
         GIT_TERMINAL_PROMPT: "0",
         GIT_SSH_COMMAND: process.env.GIT_SSH_COMMAND ?? "ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes",
         // ssh-agent, when the service user has one.
         ...(process.env.SSH_AUTH_SOCK ? { SSH_AUTH_SOCK: process.env.SSH_AUTH_SOCK } : {}),
         ...(process.env.SSH_AGENT_PID ? { SSH_AGENT_PID: process.env.SSH_AGENT_PID } : {}),
         // Some ssh configurations resolve the default identity via these.
         ...(process.env.USER ? { USER: process.env.USER } : {}),
         ...(process.env.LOGNAME ? { LOGNAME: process.env.LOGNAME } : {}),
      };
   }

   private runBoth(cmd: string, args: string[], cwd: string, onLog?: (chunk: string) => void): Promise<{ stdout: string; stderr: string }> {
      const stripAnsi = (s: string): string => s.replace(/\x1B\[[0-9;]*[a-zA-Z]/g, "");
      return new Promise((resolve, reject) => {
         const proc = spawn(cmd, args, {
            cwd,
            stdio: ["ignore", "pipe", "pipe"],
            env: this.childEnv(),
         });
         let stdout = "";
         let stderr = "";

         proc.stdout.on("data", (d: Buffer) => {
            const text = stripAnsi(d.toString());
            stdout += text;
            onLog?.(text);
         });
         proc.stderr.on("data", (d: Buffer) => {
            const text = stripAnsi(d.toString());
            stderr += text;
            // docker build writes its progress to stderr; surface it as normal output.
            onLog?.(text);
         });

         proc.on("error", reject);
         proc.on("close", (code) => {
            if (code === 0) resolve({ stdout, stderr });
            else reject(new Error(`${cmd} ${args[0]} exited with code ${code}${stderr.trim() ? `: ${stderr.trim().split("\n").slice(-3).join(" ")}` : ""}`));
         });
      });
   }
}
