import { Global, type MiddlewareConsumer, Module, RequestMethod, Scope } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { AppController } from "./app.controller";
import { HealthController } from "./health/health.controller";
import { AppService } from "./app.service";
import { AuthModule } from "./auth/auth.module";
import { FilesModule } from "./files/files.module";
import { DirectoriesModule } from "./directories/directories.module";
import { TempUrlModule } from "./temp-url/temp-url.module";
import { RequestContextModule } from "nestjs-request-context";
import { AdminModule } from "./admin/admin.module";
import { UserProfileModule } from "./user-profile/user-profile.module";
import { ThrottlerModule } from "@nestjs/throttler";
import { RealIpThrottlerGuard } from "./real-ip-throttler.guard";
import { SignedServiceSerializer } from "./auth/service-auth.util";
import { APP_GUARD, INQUIRER } from "@nestjs/core";
import { CORPMiddleware } from "./corp.middleware";
import { CsrfMiddleware } from "./csrf.middleware";
import { TrafficMiddleware } from "./traffic.middleware";
import { TrafficService } from "./traffic.service";
import { AppLogger } from "./logging";
import { AbstractFileSystem } from "./file-system/abstract-file-system.interface";
import { NodeFileSystemService } from "./file-system/file-system.service";
import { InstrumentedFileSystemService } from "./file-system/instrumented-file-system.service";
import { TieredStorageService } from "./file-system/tiered-storage.service";
import { MetricsPusherService, METRICS_SERVICE } from "./metrics-pusher.service";
import { HeartbeatService } from "./heartbeat.service";
import { ScheduleModule } from "@nestjs/schedule";
import { ClientsModule, Transport } from "@nestjs/microservices";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { EnvVariables, ReplicationRole, validate } from "./config/config.validator";
import { isDev, REDIS_CACHE } from "./util";
import Redis from "ioredis";
import { FeatureFlagService } from "./admin/feature-flag.service";
import { FeatureFlag } from "./models/admin/featureFlag";
import { ReplicationModule } from "./replication/replication.module";
import { ReplicaLinkRegistry } from "./replication/replica-link.registry";
import { ReplicationGateway } from "./replication/replication.gateway";
import yamlConfigLoader from "./config/config.loader";
import { GoogleBackupModule } from "./replication/google-backup.module";

@Global()
@Module({
   imports: [
      TypeOrmModule.forFeature([FeatureFlag]),
      ClientsModule.registerAsync([
         {
            name: METRICS_SERVICE,
            useFactory: (config: ConfigService<EnvVariables>) => ({
               transport: Transport.TCP,
               options: {
                  host: config.get("cross-service.metrics-api.host", { infer: true }) || "127.0.0.1",
                  port: config.get("cross-service.metrics-api.port.tcp", { infer: true }) ?? 14002,
                  serializer: new SignedServiceSerializer(config.get("cross-service.secret", { infer: true })),
               },
            }),
            inject: [ConfigService],
         },
      ]),
   ],
   providers: [
      {
         provide: AppLogger,
         scope: Scope.TRANSIENT,
         inject: [INQUIRER, ConfigService],
         useFactory: (parentClass: object, config: ConfigService<EnvVariables>) => {
            return new AppLogger(parentClass?.constructor.name ?? "UnknownSource", config);
         },
      },
      {
         provide: AbstractFileSystem,
         useClass: InstrumentedFileSystemService,
      },
      NodeFileSystemService,
      TieredStorageService,
      MetricsPusherService,
      HeartbeatService,
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
      FeatureFlagService,
      TrafficService,
      ReplicaLinkRegistry,
      ReplicationGateway,
   ],
   exports: [AppLogger, AbstractFileSystem, REDIS_CACHE, FeatureFlagService, TrafficService, MetricsPusherService, TieredStorageService, ClientsModule, ReplicaLinkRegistry],
})
export class GlobalUtilityModule { }

@Module({
   imports: [
      GlobalUtilityModule,
      ConfigModule.forRoot({
         ignoreEnvFile: true,
         load: [yamlConfigLoader],
         isGlobal: true,
      }),
      RequestContextModule,
      AuthModule,
      TypeOrmModule.forRootAsync({
         useFactory: (config: ConfigService<EnvVariables>) => {
            return {
               type: config.get("db.type", { infer: true }) as any,
               host: config.get("db.host", { infer: true }),
               port: config.get("db.port", { infer: true }),
               username: config.get("db.username", { infer: true }),
               password: config.get("db.password", { infer: true }),
               database: config.get<string>("db.name", { infer: true }),
               entities: ["dist/src/models/**/*{.ts,.js}"],
               synchronize: isDev(config),
               logging: false,
               extra: { timezone: "+00:00" },
               // TODO: use "node-redis"
               // For node-redis v4+ (otherwise issues with docker)
               // socket: { host: "redis-service-name", port: 6379 }
               cache: {
                  type: "ioredis",
                  duration: 1000, // 1 second
                  options: {
                     host: config.get("redis.host", { infer: true }),
                     port: config.get("redis.port", { infer: true }),
                     password: config.get("redis.password", { infer: true }),
                  },
                  alwaysEnabled: true,
               },
            };
         },
         inject: [ConfigService],
      }),
      ThrottlerModule.forRoot([{
         // ttl is in MILLISECONDS in @nestjs/throttler v5+. The previous value (30) meant a
         // 30ms window, i.e. the limit reset ~33x/second and never actually throttled.
         // 1000 requests / minute per real client IP is a generous global ceiling that still
         // stops brute-force/scraping abuse; sensitive routes tighten this further via @Throttle.
         ttl: 60_000,
         limit: 1000,
      }]),
      FilesModule,
      DirectoriesModule,
      TempUrlModule,
      AdminModule,
      UserProfileModule,
      ReplicationModule,
      GoogleBackupModule,
      ScheduleModule.forRoot(),
   ],
   controllers: [AppController, HealthController],
   providers: [
      AppService,
      {
         provide: APP_GUARD,
         useClass: RealIpThrottlerGuard,
      },
   ],
})
export class AppModule {
   configure(consumer: MiddlewareConsumer) {
      consumer.apply(CORPMiddleware, CsrfMiddleware, TrafficMiddleware).forRoutes({ path: "*", method: RequestMethod.ALL });
   }
}
