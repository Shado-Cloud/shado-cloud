import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { io, type Socket as ClientSocket } from "socket.io-client";
import * as path from "path";
import * as os from "os";
import { EnvVariables, ReplicationRole } from "src/config/config.validator";
import { AbstractFileSystem } from "src/file-system/abstract-file-system.interface";
import { signServiceHeaders } from "src/auth/service-auth.util";
import {
   DEPLOY_EVENT,
   DEPLOY_PROGRESS_EVENT,
   HAS_FILE_EVENT,
   REPLICA_LINK_NAMESPACE,
   type HasFileReply,
   type HasFileRequest,
   type ReplicaDeployAck,
   type ReplicaDeployProgress,
   type ReplicaDeployRequest,
   type ReplicaMirrorReport,
} from "./replica-link.constants";
import { ReplicaDeployRunner } from "./replica-deploy.runner";

/**
 * Replica-side endpoint of the replica-link. Only active when this node's replication
 * role is Replica. Dials out to the master (which is publicly reachable) and answers
 * live "do you have this file?" queries by checking its own cloud-dir and configured
 * mirror disks. Socket.IO handles reconnection automatically.
 *
 * It also accepts deployment orders from the master and streams the resulting step status
 * and log output back over the same socket (see {@link ReplicaDeployRunner}).
 */
@Injectable()
export class ReplicaLinkClient implements OnModuleInit, OnModuleDestroy {
   private readonly logger = new Logger(ReplicaLinkClient.name);
   private socket?: ClientSocket;

   constructor(
      private readonly config: ConfigService<EnvVariables>,
      @Inject() private readonly fs: AbstractFileSystem,
      @Inject() private readonly deployRunner: ReplicaDeployRunner,
   ) {}

   onModuleInit(): void {
      const role = this.config.get("this-service.replication.role", { infer: true });
      if (role !== ReplicationRole.Replica) return;

      const masterHost = this.config.get("this-service.replication.master-or-replica-ip", { infer: true });
      if (!masterHost) {
         this.logger.error("Replica-link: master IP is not configured; not connecting");
         return;
      }

      const protocol = masterHost.includes("shadijiha.com") ? "https" : "http";
      const url = `${protocol}://${masterHost}`;
      const mirrorDirs = (this.config.get("this-service.replication.mirror-dirs", { infer: true }) ?? []).length;
      const secret = this.config.get("cross-service.secret", { infer: true });

      this.logger.log(`Replica-link: connecting to master at ${url}${REPLICA_LINK_NAMESPACE}`);

      this.socket = io(`${url}${REPLICA_LINK_NAMESPACE}`, {
         transports: ["websocket"],
         reconnection: true,
         reconnectionDelay: 5000,
         // `auth` is a function so a FRESH, time-bound HMAC is generated on every
         // (re)connection attempt — a stale signature would fail the 5-minute window.
         // The raw secret is never sent: signServiceHeaders' legacy x-service-key is stripped.
         auth: (cb: (data: Record<string, unknown>) => void) => {
            const headers = signServiceHeaders(secret) as Record<string, string>;
            delete headers["x-service-key"];
            // role: "app" — this client answers file queries. The separate updater container
            // declares "updater" and receives the deployment orders, because the app container
            // is the thing being replaced and cannot report on its own replacement.
            cb({ ...headers, deviceName: os.hostname(), mirrorDirs, role: "app" });
         },
      });

      this.socket.on("connect", () => this.logger.log("Replica-link: connected to master"));
      this.socket.on("connect_error", (err) => this.logger.warn(`Replica-link: connect error: ${err.message}`));
      this.socket.on("disconnect", (reason) => this.logger.warn(`Replica-link: disconnected (${reason})`));

      // Master asks whether we currently have a file; reply with a live filesystem check.
      this.socket.on(HAS_FILE_EVENT, (req: HasFileRequest, ack?: (reply: HasFileReply) => void) => {
         const reply = this.checkFile(req?.path ?? "");
         if (typeof ack === "function") ack(reply);
      });

      // Master orders us to deploy. The order carries no commands — we run OUR pipeline and
      // stream progress back over the same socket.
      this.socket.on(DEPLOY_EVENT, (req: ReplicaDeployRequest, ack?: (reply: ReplicaDeployAck) => void) => {
         let reply: ReplicaDeployAck;
         try {
            reply = this.deployRunner.accept(req, (progress: ReplicaDeployProgress) => {
               this.socket?.emit(DEPLOY_PROGRESS_EVENT, progress);
            });
         } catch (e) {
            this.logger.error(`Failed to accept deployment order: ${(e as Error).message}`);
            reply = { accepted: false, reason: (e as Error).message };
         }
         if (!reply.accepted) this.logger.warn(`Refused deployment order ${req?.runId}: ${reply.reason}`);
         if (typeof ack === "function") ack(reply);
      });
   }

   /** Live check of the replica's own cloud-dir + mirror disks for a cloud-dir-relative path. */
   private checkFile(relPath: string): HasFileReply {
      const safe = (p: string): boolean => {
         try {
            return this.fs.existsSync(p);
         } catch {
            return false;
         }
      };

      const cloudDir = this.config.get("this-service.cloud-dir", { infer: true });
      const mirrorDirs = this.config.get("this-service.replication.mirror-dirs", { infer: true }) ?? [];

      const mirrors: ReplicaMirrorReport[] = mirrorDirs.map((dir) => ({
         dir,
         // Only count the file as mirrored if the disk itself is mounted (root exists).
         present: safe(dir) && safe(path.join(dir, relPath)),
      }));

      return {
         cloudDir: relPath ? safe(path.join(cloudDir, relPath)) : false,
         mirrors,
      };
   }

   onModuleDestroy(): void {
      this.socket?.disconnect();
   }
}
