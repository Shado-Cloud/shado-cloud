import { MiddlewareConsumer, Module, NestModule, Scope } from "@nestjs/common";
import { ReplicationService } from "./replication.service";
import { GoogleDriveBackupService } from "./google-drive-backup.service";
import { TrustedIpMiddleware } from "./trusted-ip.middleware";
import { ReplicationController } from "./replication.controller";
import { HealthController } from "src/health/health.controller";
import { ReplicaLinkClient } from "./replica-link.client";
import { ReplicaDeployRunner } from "./replica-deploy.runner";
import { ImageArtifactService } from "./image-artifact.service";
import { ConditionalModule, ConfigModule, ConfigService } from "@nestjs/config";
import { AbstractFileSystem } from "src/file-system/abstract-file-system.interface";
import { NodeFileSystemService } from "src/file-system/file-system.service";
import { ScheduleModule } from "@nestjs/schedule";
import yamlConfigLoader from "../config/config.loader";
import { ServiceKeyGuard } from "src/auth/service-key.guard";
import { EnvVariables } from "src/config/config.validator";
import { EmailService } from "src/admin/email.service";
import { REDIS_CACHE } from "src/util";
import Redis from "ioredis";
import { ClientsModule, Transport } from "@nestjs/microservices";
import { AUTH_SERVICE } from "src/auth/auth.constants";
import { SignedServiceSerializer } from "src/auth/service-auth.util";

/**
 * Replication module — keeps a secondary "replica" node in sync with the primary
 * "master" node, and exposes the endpoints that make it work.
 *
 *   ┌──────────────────────┐     pull over HTTPS (replica-initiated)     ┌──────────────────────────┐
 *   │  Master (source of   │  ◀──────────────────────────────────────    │  Replica (behind NAT /    │
 *   │  truth, public URL)  │     files + DB dump, encrypted in transit    │  Cloudflare tunnel)       │
 *   │                      │  ────────────────────────────────────────▶  │                           │
 *   └──────────────────────┘     has-file queries over the replica-link   └──────────────────────────┘
 *                                socket (master-initiated, replica-answered)
 *
 * Pieces:
 *  - {@link ReplicationService}  the sync engine: the replica pulls the master's files
 *                                and database on a cron; the master serves them.
 *  - {@link ReplicationController} the HTTP endpoints the replica pulls from.
 *  - {@link ReplicaLinkClient}   replica-side socket that answers live "do you have this
 *                                file?" queries from the master (used by the backups API),
 *                                and accepts deployment orders from it.
 *  - {@link ReplicaDeployRunner} runs the replica's own deployment pipeline when ordered,
 *                                streaming step status and logs back to the master.
 *  - {@link TrustedIpMiddleware} restricts the HTTP endpoints to allow-listed IPs.
 *
 * This module is also booted STANDALONE as the whole app when the node's role is
 * `replica` (see main.ts) — hence it declares its own ConfigModule / Redis / filesystem
 * providers rather than relying on the master's global module.
 */
@Module({
   imports: [
      ConfigModule.forRoot({
         ignoreEnvFile: true,
         load: [yamlConfigLoader],
         isGlobal: true,
      }),
      ScheduleModule.forRoot(),
      ClientsModule.registerAsync([
         {
            name: AUTH_SERVICE,
            useFactory: (config: ConfigService<EnvVariables>) => ({
               transport: Transport.TCP,
               options: {
                  host: config.get("cross-service.auth-api.host", { infer: true }),
                  port: config.get("cross-service.auth-api.port.tcp", { infer: true }) ?? 11002,
                  serializer: new SignedServiceSerializer(config.get("cross-service.secret", { infer: true })),
               },
            }),
            inject: [ConfigService],
         },
      ]),
   ],
   // HealthController: the only unauthenticated route a replica exposes. The updater polls
   // it after swapping the app container and rolls back if it does not come up healthy.
   controllers: [ReplicationController, HealthController],
   providers: [
      ReplicationService,
      ReplicaLinkClient,
      ReplicaDeployRunner,
      ImageArtifactService,
      {
         provide: AbstractFileSystem,
         useClass: NodeFileSystemService,
      },
      ServiceKeyGuard,
      EmailService,
      {
         provide: REDIS_CACHE,
         useFactory: (config: ConfigService<EnvVariables>) => {
            return new Redis({
               host: config.get("redis.host", { infer: true }),
               port: config.get("redis.port", { infer: true }),
               password: config.get("redis.password", { infer: true }),
            });
         },
         scope: Scope.DEFAULT,
         inject: [ConfigService],
      },

   ],
   exports: [ImageArtifactService],
})
export class ReplicationModule implements NestModule {
   configure(consumer: MiddlewareConsumer) {
      consumer.apply(TrustedIpMiddleware).forRoutes("replication");
   }
}
