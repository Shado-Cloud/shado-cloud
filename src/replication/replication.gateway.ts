import { Inject, Logger } from "@nestjs/common";
import { ConnectedSocket, MessageBody, OnGatewayConnection, OnGatewayDisconnect, SubscribeMessage, WebSocketGateway } from "@nestjs/websockets";
import type { Socket } from "socket.io";
import { ConfigService } from "@nestjs/config";
import { EnvVariables } from "src/config/config.validator";
import { ReplicaLinkRegistry } from "./replica-link.registry";
import { DEPLOY_PROGRESS_EVENT, REPLICA_LINK_NAMESPACE, type ReplicaDeployProgress, type ReplicaLinkRole } from "./replica-link.constants";
import { verifyServiceHmac } from "src/auth/service-auth.util";

/**
 * Master-side endpoint of the replica-link. Replicas connect here (outbound from their
 * side, so NAT/tunnel is not a problem) and are tracked in ReplicaLinkRegistry. The
 * connection is authenticated with the shared cross-service secret; unauthenticated
 * sockets are dropped immediately.
 *
 * Two flows run over it: master-initiated queries/orders (has-file, deploy) and the
 * replica-initiated progress stream that reports a deployment order's status and logs.
 *
 * On a replica node this gateway is still instantiated but simply never receives
 * connections (the replica isn't publicly reachable) — harmless.
 */
@WebSocketGateway({
   namespace: REPLICA_LINK_NAMESPACE,
   cors: { origin: true, credentials: true },
})
export class ReplicationGateway implements OnGatewayConnection, OnGatewayDisconnect {
   private readonly logger = new Logger(ReplicationGateway.name);

   constructor(
      private readonly config: ConfigService<EnvVariables>,
      private readonly registry: ReplicaLinkRegistry,
   ) {}

   handleConnection(client: Socket): void {
      const auth = (client.handshake.auth ?? {}) as Record<string, unknown>;
      const expected = this.config.get("cross-service.secret", { infer: true });

      // Same HMAC scheme as ServiceKeyGuard: a time-bound (5 min), nonce'd signature over
      // an empty body. The raw secret is never transmitted — a captured handshake can't be
      // replayed beyond the window.
      if (!verifyServiceHmac(expected, auth as Record<string, any>, "")) {
         this.logger.warn(`Rejected replica-link connection ${client.id}: invalid service signature`);
         client.disconnect();
         return;
      }

      const ip =
         (client.handshake.headers["cf-connecting-ip"] as string) ||
         (client.handshake.headers["x-forwarded-for"] as string)?.split(",")[0]?.trim() ||
         client.handshake.address;
      const deviceName = typeof auth.deviceName === "string" && auth.deviceName.trim() ? auth.deviceName.trim() : "unknown";
      const mirrorDirs = Number.isFinite(Number(auth.mirrorDirs)) ? Number(auth.mirrorDirs) : 0;

      // Two clients per replica host share this namespace: the app (file queries) and the
      // updater (deployments). Deploy orders are routed by this, so an unrecognised value must
      // fall back to "app" rather than be trusted blindly.
      const role: ReplicaLinkRole = auth.role === "updater" ? "updater" : "app";

      this.registry.register(client, ip, deviceName, mirrorDirs, role);
   }

   handleDisconnect(client: Socket): void {
      this.registry.unregister(client.id);
   }

   /**
    * Progress frames from a replica that is running a deployment order. Only sockets already
    * in the registry are trusted — an unregistered socket never passed the HMAC handshake, so
    * the registry drops its frames.
    */
   @SubscribeMessage(DEPLOY_PROGRESS_EVENT)
   handleDeployProgress(@ConnectedSocket() client: Socket, @MessageBody() progress: ReplicaDeployProgress): void {
      if (!progress || typeof progress.runId !== "string" || typeof progress.phase !== "string") {
         this.logger.debug(`Dropping malformed deploy progress frame from socket ${client.id}`);
         return;
      }
      this.registry.handleDeployProgress(client.id, progress);
   }
}
