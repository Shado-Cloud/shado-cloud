import { Body, Controller, Get, Param, Req, Res, UseGuards } from "@nestjs/common";
import { ReplicationService, LISTING_COMPLETE_HEADER } from "./replication.service";
import { ApiTags } from "@nestjs/swagger";
import { SkipThrottle } from "@nestjs/throttler";
import { ServiceKeyGuard } from "src/auth/service-key.guard";
import { Request, Response } from "express";
import { resolveClientIp } from "./client-ip.util";
import { ImageArtifactService } from "./image-artifact.service";

/**
 * HTTP endpoints the REPLICA pulls from (all master-served). Every route is
 * service-to-service only: authenticated by {@link ServiceKeyGuard} (per-request HMAC)
 * and additionally restricted to allow-listed IPs by TrustedIpMiddleware (wired in the
 * module). Throttling is skipped because a single sync legitimately pulls thousands of
 * files from one peer IP, which the public per-IP rate limit would otherwise block.
 */
@Controller("replication")
@ApiTags("Replication")
@SkipThrottle()
export class ReplicationController {
   constructor(
      private readonly replicationService: ReplicationService,
      private readonly imageArtifacts: ImageArtifactService,
   ) {}

   /** Master: list every file in cloud-dir, and record the calling replica in the registry. */
   @Get("listall")
   @UseGuards(ServiceKeyGuard)
   public async listall(@Req() req: Request, @Res({ passthrough: true }) res: Response) {
      // The replica self-reports its device name and mirror-disk count via headers; the
      // master combines device name + resolved client IP to identify it (handles two
      // replicas behind one IP). IP honors CF-Connecting-IP behind a tunnel.
      const header = (name: string): string | undefined => {
         const v = req.headers[name];
         return Array.isArray(v) ? v[0] : v;
      };
      const deviceName = header("x-replica-device");
      const rawMirrors = header("x-replica-mirrors");
      const mirrorDirs = rawMirrors !== undefined ? parseInt(rawMirrors, 10) : undefined;
      await this.replicationService.recordReplicaRequest(
         resolveClientIp(req),
         req.headers["user-agent"],
         Number.isFinite(mirrorDirs) ? mirrorDirs : undefined,
         deviceName,
      );
      // Tell the replica whether this listing is trustworthy enough to delete from. An entry the
      // master cannot resolve (a cold-tiered file on an unmounted drive) is absent from the body but
      // is not deleted, and the replica must not unlink its copy on that basis.
      const { files, complete } = await this.replicationService.listCloudDirDetailed();
      res.setHeader(LISTING_COMPLETE_HEADER, complete ? "1" : "0");
      return files;
   }

   /** Manually trigger a replication pass (normally driven by the per-minute cron). */
   @Get("sync")
   @UseGuards(ServiceKeyGuard)
   public async sync() {
      return this.replicationService.replicate();
   }

   /** Master: stream one file, encrypted, to the replica. */
   @Get("getfile/:path")
   @UseGuards(ServiceKeyGuard)
   public async getFile(@Param("path") path: string, @Res() res: Response) {
      return this.replicationService.getFile(path, res);
   }

   /** Master: stream an encrypted dump of all databases to the replica. */
   @Get("database")
   @UseGuards(ServiceKeyGuard)
   public async getDatabase(@Res() res: Response) {
      return this.replicationService.getDatabaseDump(res);
   }

   /**
    * Master: stream an encrypted container image the replica was told to deploy.
    *
    * Replicas are updated by image replacement rather than by building from source, and the
    * images come from here instead of a registry — bandwidth is not the constraint, and this
    * keeps the code off any third party and reuses an already-authenticated path.
    *
    * The artifact id is the tarball's content hash, which the replica received over the
    * HMAC-authenticated replica-link and verifies before letting the archive near its Docker
    * daemon. Same guard and IP allow-list as every other route here.
    */
   @Get("image/:artifact")
   @UseGuards(ServiceKeyGuard)
   public getImage(@Param("artifact") artifact: string, @Res() res: Response) {
      this.imageArtifacts.stream(artifact, res);
   }
}
