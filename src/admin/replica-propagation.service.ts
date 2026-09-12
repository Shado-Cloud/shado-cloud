import { Injectable, Logger } from "@nestjs/common";
import type { Subscription } from "rxjs";
import { ReplicaLinkRegistry, type ReplicaLinkInfo } from "src/replication/replica-link.registry";
import type { ReplicaLinkRole } from "src/replication/replica-link.constants";
import type { ReplicaDeployProgress, ReplicaDeployRequest, ReplicaImageRef } from "src/replication/replica-link.constants";

/** Lifecycle of one replica inside a propagation run. */
export type ReplicaRunStatus =
   /** Order sent, waiting for the replica to accept it. */
   | "dispatching"
   /** The replica refused the order (disabled, already deploying, no pipeline). */
   | "rejected"
   /** The replica never acknowledged the order. */
   | "unreachable"
   /** Accepted and executing its pipeline. */
   | "running"
   /** Finished its pipeline and is restarting; we're waiting for it to come back online. */
   | "restarting"
   | "success"
   | "failed"
   /** Accepted, then went quiet for longer than the run timeout. */
   | "timeout";

export interface ReplicaStepState {
   step: string;
   name: string;
   status: "pending" | "running" | "success" | "failed";
   startedAt?: number;
   finishedAt?: number;
   error?: string;
}

/** Everything the primary knows about one replica's deployment. */
export interface ReplicaRunState {
   /** Replica-link socket id — unique within this run. */
   id: string;
   deviceName: string;
   ip: string;
   status: ReplicaRunStatus;
   /** Why it was rejected / unreachable / failed. */
   reason?: string;
   /** Directory the replica is deploying in, as reported in its ack. */
   workDir?: string;
   /** The replica's own pipeline. */
   steps: ReplicaStepState[];
   currentStep?: string;
   /** Accumulated log output from the replica (trimmed to the tail — see MAX_OUTPUT_CHARS). */
   output: string;
   startedAt?: number;
   finishedAt?: number;
}

export interface ReplicaPropagationState {
   runId: string;
   dispatchedAt: number;
   finishedAt?: number;
   /** Replicas that were online when the step ran. Empty = nothing to propagate to. */
   replicas: ReplicaRunState[];
}

export interface PropagationCallbacks {
   /**
    * Fires once, as soon as the replica list and their accept/reject answers are known. The
    * state object handed over is the SAME one that gets mutated for the rest of the run and
    * returned at the end, so a caller can hold the reference instead of re-snapshotting.
    */
   onDispatch: (state: ReplicaPropagationState) => void;
   /** A replica-level status/step transition. Cheap to serialize; safe to persist. */
   onReplicaUpdate: (replica: ReplicaRunState) => void;
   /** A log delta for one replica. High frequency — stream it, don't persist per call. */
   onReplicaOutput: (replicaId: string, output: string) => void;
   /** A line about the propagation itself (dispatch summary, timeouts) for the step log. */
   onLog: (line: string) => void;
}

export interface PropagationOptions {
   /** Deployment id, used to build the run id. */
   deploymentId: string;
   project: string;
   branch?: string;
   triggeredBy?: string;
   /** How long a replica may take to finish its pipeline before it's marked `timeout`. */
   timeoutMs?: number;
   /** How long to wait for a restarting replica to reconnect before marking it failed. */
   restartGraceMs?: number;
   /** How long to wait for a replica to acknowledge the order. */
   dispatchTimeoutMs?: number;
   /**
    * How long to wait for replicas to appear before giving up and treating the step as a
    * no-op. Matters when this step runs AFTER the primary's own restart: the primary just
    * dropped every replica-link socket, and replicas re-dial on their own reconnect delay,
    * so at t=0 the registry is legitimately empty.
    */
   waitForReplicasMs?: number;
   /**
    * Quiet period after the last replica connects before dispatching. Replicas reconnect
    * independently, so without it the first one back would win the race and its peers would
    * be left out of the deployment.
    */
   replicaSettleMs?: number;
   /**
    * Images staged by an earlier `buildImage` step. When present the order is an IMAGE
    * deployment: replicas download and swap these rather than building from source.
    */
   images?: ReplicaImageRef[];
}

/** A replica host, aggregated across however many replica-link clients it has connected. */
export interface ConnectedReplicaHost {
   deviceName: string;
   ip: string;
   mirrorDirs: number;
   /** Earliest connection among this host's clients. */
   connectedAt: number;
   /** Which halves are present. `["updater"]` means the app image has not arrived yet. */
   roles: ReplicaLinkRole[];
}
const MAX_OUTPUT_CHARS = 64 * 1024;
const DEFAULT_TIMEOUT_MS = 15 * 60 * 1000;
const DEFAULT_RESTART_GRACE_MS = 3 * 60 * 1000;
const DEFAULT_DISPATCH_TIMEOUT_MS = 20 * 1000;
const DEFAULT_WAIT_FOR_REPLICAS_MS = 90 * 1000;
const DEFAULT_REPLICA_SETTLE_MS = 5 * 1000;

const TERMINAL_STATUSES: ReadonlySet<ReplicaRunStatus> = new Set<ReplicaRunStatus>(["success", "failed", "rejected", "unreachable", "timeout"]);

/** Per-replica timer bookkeeping for one propagation run. */
interface ReplicaWatcher {
   /** Resolve the replica's wait promise and clear its timers. */
   finish: () => void;
   /** Swap the long pipeline timeout for the shorter post-restart reconnect window. */
   rearmForRestart: () => void;
}

/**
 * Master-side orchestrator for the "Propagate to Replicas" deployment step.
 *
 * Sends every connected replica an order to deploy itself over the replica-link, then
 * aggregates their step transitions and log output into a single {@link ReplicaPropagationState}
 * that the deployment SSE stream and the admin UI render.
 *
 * The order carries no commands: each replica runs the pipeline from its own config. The
 * final step of a replica's pipeline typically restarts it, which severs the link — so a
 * replica that reports `restarting` is only marked successful once it reconnects, which
 * doubles as a health check.
 */
@Injectable()
export class ReplicaPropagationService {
   private readonly logger = new Logger(ReplicaPropagationService.name);

   constructor(private readonly registry: ReplicaLinkRegistry) {}

   /**
    * Replica HOSTS currently connected, aggregated across their clients.
    *
    * Counting hosts rather than sockets, and including updater-only hosts, because a freshly
    * provisioned replica has an updater connected and no app — it has not been sent an image yet.
    * Reporting that as "no replicas online" is both wrong and impossible to diagnose from the UI:
    * the operator sees nothing while their replica sits there connected and waiting.
    */
   public connectedReplicas(): ConnectedReplicaHost[] {
      const byHost = new Map<string, ConnectedReplicaHost>();
      for (const client of this.registry.list("any")) {
         const key = `${client.ip}|${client.deviceName}`;
         const existing = byHost.get(key);
         if (existing) {
            if (!existing.roles.includes(client.role)) existing.roles.push(client.role);
            existing.connectedAt = Math.min(existing.connectedAt, client.connectedAt);
            existing.mirrorDirs = Math.max(existing.mirrorDirs, client.mirrorDirs);
         } else {
            byHost.set(key, {
               deviceName: client.deviceName,
               ip: client.ip,
               mirrorDirs: client.mirrorDirs,
               connectedAt: client.connectedAt,
               roles: [client.role],
            });
         }
      }
      return [...byHost.values()];
   }

   /**
    * Run the propagation to completion. Resolves once every replica has reached a terminal
    * status (or timed out) — never rejects, so a replica problem can't crash the pipeline.
    */
   public async propagate(opts: PropagationOptions, cb: PropagationCallbacks): Promise<ReplicaPropagationState> {
      const runId = `${opts.deploymentId}_prop_${Date.now()}`;
      const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
      const restartGraceMs = opts.restartGraceMs ?? DEFAULT_RESTART_GRACE_MS;
      const dispatchTimeoutMs = opts.dispatchTimeoutMs ?? DEFAULT_DISPATCH_TIMEOUT_MS;

      const state: ReplicaPropagationState = { runId, dispatchedAt: Date.now(), replicas: [] };
      const byId = new Map<string, ReplicaRunState>();

      // An image order is applied by the updater; a source order by the app itself. Wait for
      // whichever will actually receive it — waiting on the wrong one would time out while the
      // right one sat there connected.
      const targetRole: ReplicaLinkRole = opts.images?.length ? "updater" : "app";
      const online = await this.awaitReplicas(
         opts.waitForReplicasMs ?? DEFAULT_WAIT_FOR_REPLICAS_MS,
         opts.replicaSettleMs ?? DEFAULT_REPLICA_SETTLE_MS,
         targetRole,
         cb,
      );
      if (online.length === 0) {
         cb.onLog("No replicas are connected to the replica-link — nothing to propagate to.\n");
         state.finishedAt = Date.now();
         cb.onDispatch(state);
         return state;
      }

      cb.onLog(`Dispatching deployment order ${runId} to ${online.length} connected replica(s):\n`);
      for (const r of online) cb.onLog(`  - ${r.deviceName} (${r.ip})\n`);

      const request: ReplicaDeployRequest = {
         runId,
         project: opts.project,
         branch: opts.branch,
         triggeredBy: opts.triggeredBy,
         images: opts.images?.length ? opts.images : undefined,
      };

      if (request.images) {
         cb.onLog(`Image deployment — ${request.images.length} image(s) to replace:\n`);
         for (const img of request.images) {
            cb.onLog(`  - ${img.service}: ${img.imageId.slice(0, 19)}… (${Math.round(img.size / 1024 / 1024)}MB)\n`);
         }
      } else {
         cb.onLog("Source deployment — each replica runs its own built-in pipeline.\n");
      }

      // Subscribe BEFORE dispatching: the replica starts running (and streaming) as it
      // answers the order, so frames can land before `dispatchDeploy` even resolves.
      const watchers = new Map<string, ReplicaWatcher>();
      const subscriptions: Subscription[] = [];
      /** Frames that arrived before the replica's bookkeeping existed; flushed after dispatch. */
      const earlyFrames: { socketId: string; progress: ReplicaDeployProgress }[] = [];
      let dispatchSettled = false;

      const settle = (replica: ReplicaRunState, status: ReplicaRunStatus, reason?: string): void => {
         if (TERMINAL_STATUSES.has(replica.status)) return;
         replica.status = status;
         if (reason) replica.reason = reason;
         replica.finishedAt = Date.now();
         replica.currentStep = undefined;
         cb.onReplicaUpdate(replica);
         watchers.get(replica.id)?.finish();
      };

      const markRestarting = (replica: ReplicaRunState): void => {
         if (TERMINAL_STATUSES.has(replica.status)) return;
         replica.status = "restarting";
         replica.currentStep = undefined;
         this.appendOutput(replica, "Waiting for the replica to reconnect to confirm the restart…\n", cb);
         cb.onReplicaUpdate(replica);
         watchers.get(replica.id)?.rearmForRestart();
      };

      subscriptions.push(
         this.registry.deployProgress$.subscribe(({ replica: from, progress }) => {
            if (progress.runId !== runId) return;
            const replica = byId.get(from.id);
            if (!replica) {
               // Either an early frame (bookkeeping not built yet) or a stray one — buffer
               // the former so the replica's first log lines and step_start aren't lost.
               if (!dispatchSettled) earlyFrames.push({ socketId: from.id, progress });
               return;
            }
            this.applyProgress(replica, progress, cb, settle, markRestarting);
         }),
      );

      // A restarting replica is only confirmed once it reconnects to the link. Subscribed
      // up front so a fast restart can't slip through before we start listening.
      subscriptions.push(
         this.registry.replicaOnline$.subscribe((info) => {
            for (const replica of state.replicas) {
               if (replica.status === "restarting" && replica.deviceName === info.deviceName && replica.ip === info.ip) {
                  this.appendOutput(replica, "Replica reconnected to the replica-link after restart.\n", cb);
                  settle(replica, "success");
               }
            }
         }),
      );

      const dispatched = await this.registry.dispatchDeploy(request, dispatchTimeoutMs);

      for (const result of dispatched) {
         const replica: ReplicaRunState = {
            id: result.replica.id,
            deviceName: result.replica.deviceName,
            ip: result.replica.ip,
            status: "dispatching",
            steps: [],
            output: "",
         };
         byId.set(replica.id, replica);
         state.replicas.push(replica);

         if (!result.ack) {
            replica.status = "unreachable";
            replica.reason = result.error ?? "No response to the deployment order";
            replica.finishedAt = Date.now();
            cb.onLog(`  ✗ ${replica.deviceName}: ${replica.reason}\n`);
            cb.onReplicaUpdate(replica);
            continue;
         }
         if (!result.ack.accepted) {
            replica.status = "rejected";
            replica.reason = result.ack.reason ?? "Refused the deployment order";
            replica.finishedAt = Date.now();
            cb.onLog(`  ✗ ${replica.deviceName}: ${replica.reason}\n`);
            cb.onReplicaUpdate(replica);
            continue;
         }

         replica.status = "running";
         replica.workDir = result.ack.workDir;
         replica.startedAt = Date.now();
         replica.steps = (result.ack.steps ?? []).map((s) => ({ step: s.step, name: s.name, status: "pending" as const }));
         cb.onLog(`  ✓ ${replica.deviceName}: accepted — ${replica.steps.length} step(s) in ${replica.workDir ?? "?"}\n`);
         cb.onReplicaUpdate(replica);
      }

      const active = state.replicas.filter((r) => r.status === "running");
      cb.onDispatch(state);

      // Replay anything that streamed in while the bookkeeping above was being built.
      dispatchSettled = true;
      for (const { socketId, progress } of earlyFrames) {
         const replica = byId.get(socketId);
         if (replica) this.applyProgress(replica, progress, cb, settle, markRestarting);
      }
      earlyFrames.length = 0;

      if (active.length === 0) {
         subscriptions.forEach((s) => s.unsubscribe());
         state.finishedAt = Date.now();
         return state;
      }

      try {
         await Promise.all(
            active.map(
               (replica) =>
                  new Promise<void>((resolve) => {
                     let settled = false;
                     let timer: NodeJS.Timeout | null = null;

                     const finish = (): void => {
                        if (settled) return;
                        settled = true;
                        if (timer) clearTimeout(timer);
                        watchers.delete(replica.id);
                        resolve();
                     };

                     const arm = (ms: number, onExpiry: () => void): void => {
                        if (timer) clearTimeout(timer);
                        timer = setTimeout(() => {
                           if (!settled) onExpiry();
                        }, ms);
                     };

                     watchers.set(replica.id, {
                        finish,
                        rearmForRestart: () =>
                           arm(restartGraceMs, () => {
                              const reason = `Did not reconnect within ${Math.round(restartGraceMs / 1000)}s of restarting`;
                              this.appendOutput(replica, `${reason}.\n`, cb);
                              settle(replica, "failed", reason);
                           }),
                     });

                     arm(timeoutMs, () => {
                        const reason = `Went quiet for more than ${Math.round(timeoutMs / 1000)}s`;
                        this.appendOutput(replica, `${reason}.\n`, cb);
                        settle(replica, "timeout", reason);
                     });

                     // A replica can reach a terminal state between building `active` and
                     // registering this watcher — don't wait on an already-finished run.
                     if (TERMINAL_STATUSES.has(replica.status)) finish();
                     else if (replica.status === "restarting") watchers.get(replica.id)?.rearmForRestart();
                  }),
            ),
         );
      } finally {
         subscriptions.forEach((s) => s.unsubscribe());
      }

      state.finishedAt = Date.now();

      const ok = state.replicas.filter((r) => r.status === "success").length;
      cb.onLog(`\nPropagation finished: ${ok}/${state.replicas.length} replica(s) deployed successfully.\n`);
      for (const r of state.replicas) {
         if (r.status !== "success") cb.onLog(`  ✗ ${r.deviceName} (${r.ip}): ${r.status}${r.reason ? ` — ${r.reason}` : ""}\n`);
      }
      this.logger.log(`Propagation ${runId}: ${ok}/${state.replicas.length} replica(s) succeeded`);

      return state;
   }

   /** True when every replica reached `success` (vacuously true with no replicas). */
   public allSucceeded(state: ReplicaPropagationState): boolean {
      return state.replicas.every((r) => r.status === "success");
   }

   /** A one-line summary for step logs, emails and the UI header. */
   public summarize(state: ReplicaPropagationState): string {
      if (state.replicas.length === 0) return "No replicas online";
      const ok = state.replicas.filter((r) => r.status === "success").length;
      return `${ok}/${state.replicas.length} replicas deployed`;
   }

   /**
    * Say why the target role is empty when the OTHER role is connected.
    *
    * Both mismatches look identical from the UI — a step that spins and then reports "no replicas"
    * — while the actual cause is a pipeline misconfiguration that is invisible from here. Worth
    * spelling out rather than leaving an operator to infer it.
    */
   private explainEmptyTarget(targetRole: ReplicaLinkRole, cb: PropagationCallbacks): void {
      const other: ReplicaLinkRole = targetRole === "app" ? "updater" : "app";
      const others = this.registry.list(other);
      if (others.length === 0) return;

      cb.onLog(`\n  Note: ${others.length} replica ${other}(s) ARE connected: ${others.map((r) => r.deviceName).join(", ")}\n`);
      if (targetRole === "app") {
         cb.onLog(
            "  This deployment carried no image, so it targets replicas that deploy themselves from source —\n" +
            "  but these replicas are image-based. Add a \"Build Replica Image\" step BEFORE this one.\n",
         );
      } else {
         cb.onLog(
            "  This deployment carried an image, which is applied by a replica's updater —\n" +
            "  but these replicas have no updater running. Provision them with the shado-replica package.\n",
         );
      }
   }

   /**
    * Resolve the set of replicas to deploy to, tolerating the reconnect window.
    *
    * When this step runs after the primary's own restart every replica-link socket was just
    * severed, so the registry starts empty and fills in as replicas re-dial. We wait for the
    * first one, then keep waiting through a quiet period so slower peers aren't left behind,
    * and give up after `waitMs` so a replica-less setup doesn't stall the pipeline.
    */
   private awaitReplicas(waitMs: number, settleMs: number, targetRole: ReplicaLinkRole, cb: PropagationCallbacks): Promise<ReplicaLinkInfo[]> {
      return new Promise((resolve) => {
         let settleTimer: NodeJS.Timeout | null = null;
         let done = false;

         const finish = (): void => {
            if (done) return;
            done = true;
            clearTimeout(hardTimer);
            if (settleTimer) clearTimeout(settleTimer);
            subscription.unsubscribe();
            resolve(this.registry.list(targetRole));
         };

         // Restarted after each new connection, so a burst of reconnects is fully collected.
         const armSettle = (): void => {
            if (settleTimer) clearTimeout(settleTimer);
            settleTimer = setTimeout(finish, settleMs);
         };

         const targetCount = (): number => this.registry.list(targetRole).length;

         const hardTimer = setTimeout(() => {
            if (targetCount() === 0) {
               cb.onLog(`Gave up waiting after ${Math.round(waitMs / 1000)}s — no replica ${targetRole} reconnected.\n`);
               this.explainEmptyTarget(targetRole, cb);
            }
            finish();
         }, waitMs);

         const subscription = this.registry.replicaOnline$.subscribe((info) => {
            if (info.role !== targetRole) return;
            cb.onLog(`Replica ${targetRole} online: ${info.deviceName} (${info.ip})\n`);
            armSettle();
         });

         if (targetCount() > 0) {
            armSettle();
         } else {
            cb.onLog(`No replica ${targetRole} online yet — waiting up to ${Math.round(waitMs / 1000)}s for it to (re)connect to the replica-link…\n`);
            this.explainEmptyTarget(targetRole, cb);
         }
      });
   }

   private applyProgress(
      replica: ReplicaRunState,
      progress: ReplicaDeployProgress,
      cb: PropagationCallbacks,
      settle: (replica: ReplicaRunState, status: ReplicaRunStatus, reason?: string) => void,
      markRestarting: (replica: ReplicaRunState) => void,
   ): void {
      switch (progress.phase) {
         case "step_start": {
            const step = this.findOrCreateStep(replica, progress.step);
            step.status = "running";
            step.startedAt = progress.at;
            replica.currentStep = step.step;
            cb.onReplicaUpdate(replica);
            break;
         }
         case "step_output": {
            if (progress.output) this.appendOutput(replica, progress.output, cb);
            break;
         }
         case "step_complete": {
            const step = this.findOrCreateStep(replica, progress.step);
            step.status = progress.status === "failed" ? "failed" : "success";
            step.finishedAt = progress.at;
            step.error = progress.error;
            cb.onReplicaUpdate(replica);
            break;
         }
         case "finished": {
            if (progress.restarting) markRestarting(replica);
            else settle(replica, progress.ok ? "success" : "failed", progress.error);
            break;
         }
      }
   }

   private findOrCreateStep(replica: ReplicaRunState, stepId?: string): ReplicaStepState {
      const id = stepId ?? "unknown";
      let step = replica.steps.find((s) => s.step === id);
      if (!step) {
         // The replica reported a step its ack didn't list (config changed mid-run).
         step = { step: id, name: id, status: "pending" };
         replica.steps.push(step);
      }
      return step;
   }

   private appendOutput(replica: ReplicaRunState, output: string, cb: PropagationCallbacks): void {
      replica.output += output;
      if (replica.output.length > MAX_OUTPUT_CHARS) {
         replica.output = `…[earlier output trimmed]…\n${replica.output.slice(-MAX_OUTPUT_CHARS)}`;
      }
      cb.onReplicaOutput(replica.id, output);
   }
}
