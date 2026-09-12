import { Injectable, Logger } from "@nestjs/common";
import { Subject } from "rxjs";
import type { Socket } from "socket.io";
import {
   DEPLOY_EVENT,
   HAS_FILE_EVENT,
   type HasFileReply,
   type HasFileRequest,
   type ReplicaDeployAck,
   type ReplicaDeployProgress,
   type ReplicaDeployRequest,
   type ReplicaLinkRole,
} from "./replica-link.constants";

interface ConnectedReplica {
   socket: Socket;
   ip: string;
   deviceName: string;
   mirrorDirs: number;
   connectedAt: number;
   /** "app" answers file queries; "updater" applies deployments. */
   role: ReplicaLinkRole;
}

/** One live replica's answer to a file query (null report = timed out / errored). */
export interface ReplicaFileResult {
   ip: string;
   deviceName: string;
   mirrorDirs: number;
   report: HasFileReply | null;
}

/** A currently-connected replica, as exposed to callers outside the registry. */
export interface ReplicaLinkInfo {
   /** Socket id — stable for the lifetime of the connection, and the key used for dispatch. */
   id: string;
   ip: string;
   deviceName: string;
   mirrorDirs: number;
   connectedAt: number;
   role: ReplicaLinkRole;
}

/** Outcome of dispatching a deployment order to one replica. */
export interface ReplicaDispatchResult {
   replica: ReplicaLinkInfo;
   /** Null when the replica never answered the order (timed out or the emit threw). */
   ack: ReplicaDeployAck | null;
   /** Set when `ack` is null. */
   error?: string;
}

/** A progress frame from a replica, tagged with which replica sent it. */
export interface ReplicaProgressEvent {
   replica: ReplicaLinkInfo;
   progress: ReplicaDeployProgress;
}

/**
 * Process-wide registry of replicas currently connected to this (master) node over the
 * replica-link socket. Held as a global provider so both the WebSocket gateway (which
 * populates it) and consumers — FilesService for the backups API, ReplicaPropagationService
 * for deployment propagation — can share it without a cross-module dependency.
 */
@Injectable()
export class ReplicaLinkRegistry {
   private readonly logger = new Logger(ReplicaLinkRegistry.name);
   private readonly replicas = new Map<string, ConnectedReplica>();

   /** Progress frames streamed by replicas running a deployment order. */
   public readonly deployProgress$ = new Subject<ReplicaProgressEvent>();

   /**
    * Fires whenever a replica (re)connects. Deployment propagation uses this to confirm that
    * a replica came back up after its restart step, since the restart severs the link.
    */
   public readonly replicaOnline$ = new Subject<ReplicaLinkInfo>();

   register(socket: Socket, ip: string, deviceName: string, mirrorDirs: number, role: ReplicaLinkRole = "app"): void {
      this.replicas.set(socket.id, { socket, ip, deviceName, mirrorDirs, connectedAt: Date.now(), role });
      this.logger.log(`Replica ${role} connected: ${deviceName} @ ${ip} (socket ${socket.id}, ${mirrorDirs} mirror disk(s)); ${this.replicas.size} online`);
      this.replicaOnline$.next(this.toInfo(socket.id, this.replicas.get(socket.id)!));
   }

   unregister(socketId: string): void {
      const existing = this.replicas.get(socketId);
      if (this.replicas.delete(socketId)) {
         this.logger.log(`Replica ${existing?.role ?? "?"} disconnected: ${existing?.deviceName ?? "?"} @ ${existing?.ip ?? "?"} (socket ${socketId}); ${this.replicas.size} online`);
      }
   }

   /** IPs of all currently-connected replica APPS (one per host, unlike updaters). */
   connectedIps(): string[] {
      return [...this.replicas.values()].filter((r) => r.role === "app").map((r) => r.ip);
   }

   /** Number of connected replica apps. Excludes updaters, which are not serving nodes. */
   connectedCount(): number {
      return [...this.replicas.values()].filter((r) => r.role === "app").length;
   }

   /**
    * Connected replicas, without exposing their sockets. Filtered by role by default to "app",
    * because that is what "a replica is online" means to a caller — an updater is tooling
    * sitting beside the replica, not a node serving anything.
    */
   list(role: ReplicaLinkRole | "any" = "app"): ReplicaLinkInfo[] {
      return [...this.replicas.entries()]
         .filter(([, entry]) => role === "any" || entry.role === role)
         .map(([id, entry]) => this.toInfo(id, entry));
   }

   /** Route a progress frame from a replica socket onto {@link deployProgress$}. */
   handleDeployProgress(socketId: string, progress: ReplicaDeployProgress): void {
      const entry = this.replicas.get(socketId);
      if (!entry) {
         this.logger.debug(`Dropping deploy progress from unknown socket ${socketId}`);
         return;
      }
      this.deployProgress$.next({ replica: this.toInfo(socketId, entry), progress });
   }

   /**
    * Order every connected replica to deploy itself, in parallel. The returned acks only say
    * whether each replica ACCEPTED; the run itself streams back on {@link deployProgress$}.
    *
    * The order carries no commands — each replica runs its own configured pipeline.
    */
   async dispatchDeploy(req: ReplicaDeployRequest, timeoutMs = 15000): Promise<ReplicaDispatchResult[]> {
      // Route by role. An IMAGE order is for updaters — they own the container swap, and the app
      // container is the thing being replaced. A SOURCE order is for apps, which deploy
      // themselves. Sending either to both would have two processes deploying the same replica
      // at once, in different ways.
      const targetRole: ReplicaLinkRole = req.images?.length ? "updater" : "app";
      const entries = [...this.replicas.entries()].filter(([, e]) => e.role === targetRole);
      if (entries.length === 0) {
         this.logger.warn(`No replica "${targetRole}" clients connected; deployment order ${req.runId} reaches nobody`);
      }
      return Promise.all(
         entries.map(
            ([id, entry]) =>
               new Promise<ReplicaDispatchResult>((resolve) => {
                  const replica = this.toInfo(id, entry);
                  try {
                     entry.socket.timeout(timeoutMs).emit(DEPLOY_EVENT, req, (err: Error | null, ack: ReplicaDeployAck) => {
                        if (err) resolve({ replica, ack: null, error: `Did not acknowledge the deployment order within ${Math.round(timeoutMs / 1000)}s` });
                        else resolve({ replica, ack });
                     });
                  } catch (e) {
                     this.logger.debug(`dispatchDeploy emit failed for ${entry.deviceName} @ ${entry.ip}: ${(e as Error).message}`);
                     resolve({ replica, ack: null, error: (e as Error).message });
                  }
               }),
         ),
      );
   }

   /**
    * Ask every connected replica whether it currently has `path` (cloud-dir-relative),
    * in parallel, each bounded by `timeoutMs`. A replica that doesn't answer in time
    * yields `{ report: null }` so the caller can render it as "could not verify".
    *
    * Default is generous (15s): the replica's per-minute sync cron can block its event
    * loop with synchronous filesystem work, delaying the ack even though the check itself
    * (a few existsSync calls) is trivial.
    */
   async queryFile(path: string, timeoutMs = 15000): Promise<ReplicaFileResult[]> {
      // Apps only: an updater has no cloud-dir and cannot answer.
      const entries = [...this.replicas.values()].filter((e) => e.role === "app");
      return Promise.all(
         entries.map(
            (entry) =>
               new Promise<ReplicaFileResult>((resolve) => {
                  try {
                     entry.socket
                        .timeout(timeoutMs)
                        .emit(HAS_FILE_EVENT, { path } as HasFileRequest, (err: Error | null, reply: HasFileReply) => {
                           resolve({ ip: entry.ip, deviceName: entry.deviceName, mirrorDirs: entry.mirrorDirs, report: err ? null : reply });
                        });
                  } catch (e) {
                     this.logger.debug(`queryFile emit failed for ${entry.deviceName} @ ${entry.ip}: ${(e as Error).message}`);
                     resolve({ ip: entry.ip, deviceName: entry.deviceName, mirrorDirs: entry.mirrorDirs, report: null });
                  }
               }),
         ),
      );
   }

   private toInfo(id: string, entry: ConnectedReplica): ReplicaLinkInfo {
      return { id, ip: entry.ip, deviceName: entry.deviceName, mirrorDirs: entry.mirrorDirs, connectedAt: entry.connectedAt, role: entry.role };
   }
}
