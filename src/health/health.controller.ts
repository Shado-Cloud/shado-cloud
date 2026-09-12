import { Controller, Get, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { SkipThrottle } from "@nestjs/throttler";
import { ApiOperation, ApiTags } from "@nestjs/swagger";
import * as fs from "fs";
import * as path from "path";
import { EnvVariables, ReplicationRole } from "src/config/config.validator";

/** Deliberately minimal — this endpoint is unauthenticated. */
export interface HealthReport {
   ok: true;
   /** master | primary | replica. Which node you are talking to. */
   role: ReplicationRole | "unknown";
   /** package.json version of the running build. */
   version: string;
   /** Seconds this process has been up. Distinguishes "healthy" from "crash-looping". */
   uptime: number;
   /**
    * The container image this process was started from, when the runtime provides it
    * (SHADO_IMAGE_ID, set by the replica updater on recreate). Lets the primary detect a
    * replica running something other than what it was told to run.
    */
   image?: string;
}

/**
 * Read once at startup. `npm_package_version` is only set when started through npm, and the
 * runtime image starts node directly (`node dist/src/main`) — so fall back to the package.json
 * next to the working directory, which is /app in both the image and local dev.
 */
const VERSION: string = (() => {
   if (process.env.npm_package_version) return process.env.npm_package_version;
   try {
      return JSON.parse(fs.readFileSync(path.join(process.cwd(), "package.json"), "utf-8")).version ?? "unknown";
   } catch (e) {
      new Logger("HealthController").warn(`Could not resolve version: ${(e as Error).message}`);
      return "unknown";
   }
})();

/**
 * Liveness endpoint, registered in BOTH AppModule and ReplicationModule so it answers whichever
 * application booted (main.ts picks one based on the replication role).
 *
 * It exists for container orchestration: the replica updater polls it after swapping the app
 * container and rolls back to the previous image if it does not come up healthy. A replica runs
 * ReplicationModule, whose only other routes sit behind ServiceKeyGuard and an IP allow-list —
 * so before this there was no way to ask a replica whether it was alive.
 *
 * Unauthenticated by necessity (the caller may hold no credentials yet) and therefore
 * deliberately terse: role, version, uptime, image id. No configuration, no hostnames, no
 * counts that would describe the deployment to an unauthenticated caller.
 *
 * It also does not touch the database or Redis, on purpose. A liveness check that fails while a
 * dependency is briefly unavailable would make the updater roll back a perfectly good image.
 * Dependency health belongs in a separate, authenticated readiness check.
 */
@Controller("health")
@ApiTags("Health")
export class HealthController {
   constructor(private readonly config: ConfigService<EnvVariables>) {}

   @Get()
   @SkipThrottle()
   @ApiOperation({ summary: "Liveness probe. Unauthenticated; reports role, version and uptime." })
   public getHealth(): HealthReport {
      return {
         ok: true,
         role: this.config.get("this-service.replication.role", { infer: true }) ?? "unknown",
         version: VERSION,
         uptime: Math.round(process.uptime()),
         image: process.env.SHADO_IMAGE_ID || undefined,
      };
   }
}
