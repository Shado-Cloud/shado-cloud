import { Inject, Injectable, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { spawn } from "child_process";
import { createCipheriv, createHash, randomBytes } from "crypto";
import type { Response } from "express";
import * as os from "os";
import * as path from "path";
import { Transform } from "stream";
import { pipeline } from "stream/promises";
import { AbstractFileSystem } from "src/file-system/abstract-file-system.interface";
import { EnvVariables } from "src/config/config.validator";

/** An exported image, ready to be handed to replicas. */
export interface ImageArtifact {
   /** Opaque id used in the download URL. Derived from the content hash, so it is stable. */
   artifact: string;
   /** Docker image ID this artifact contains. */
   imageId: string;
   /** SHA-256 of the tarball, which the replica verifies before loading. */
   tarSha256: string;
   size: number;
}

/** Artifacts older than this are pruned; a deploy only needs them for minutes. */
const ARTIFACT_TTL_MS = 6 * 60 * 60 * 1000;
const ARTIFACT_PREFIX = "shado-image-";

function humanSize(bytes: number): string {
   const units = ["B", "KB", "MB", "GB"];
   let v = bytes;
   let i = 0;
   while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i++;
   }
   return `${v.toFixed(1)}${units[i]}`;
}

/**
 * Master-side store of exported container images, and the endpoint body that streams them to
 * replicas.
 *
 * Why no registry: the only reason to involve one is transfer efficiency (a registry moves just
 * the changed layers). Bandwidth is not a constraint here, and dropping it removes an external
 * dependency, the credentials to reach it, and a party that would hold the code. Replicas
 * already pull large encrypted payloads from the master — cloud-dir files and the database dump
 * — so an image is one more artifact on a path that exists and is already authenticated.
 *
 * Integrity: the tarball's SHA-256 travels over the HMAC-authenticated replica-link, separately
 * from the bytes themselves, and the replica checks it before the archive reaches its Docker
 * daemon. The image ID is then confirmed after load. A substituted or truncated download fails
 * both checks.
 */
@Injectable()
export class ImageArtifactService {
   private readonly logger = new Logger(ImageArtifactService.name);

   constructor(
      private readonly config: ConfigService<EnvVariables>,
      @Inject() private readonly fs: AbstractFileSystem,
   ) {}

   /**
    * `docker save` an image to a temp file and hash it.
    *
    * Streamed to disk rather than buffered: these are gigabytes. Hashing happens on the same
    * pass so the file is only read once.
    */
   public async export(imageId: string, onLog?: (line: string) => void): Promise<ImageArtifact> {
      this.pruneOld();

      const tmpFile = path.join(os.tmpdir(), `${ARTIFACT_PREFIX}${Date.now()}.tar`);
      onLog?.(`Exporting ${imageId} to ${tmpFile}...\n`);

      const hash = createHash("sha256");
      let size = 0;
      let lastReport = Date.now();

      const proc = spawn("docker", ["save", imageId], { stdio: ["ignore", "pipe", "pipe"] });
      let stderr = "";
      proc.stderr.on("data", (d: Buffer) => (stderr += d.toString()));

      /**
       * Hash in a Transform rather than a `data` listener on the child's stdout.
       *
       * A `data` listener puts the stream in flowing mode, which bypasses the pipe's backpressure:
       * bytes get pushed at `docker save`'s pace regardless of how fast the disk accepts them, so
       * a multi-gigabyte image accumulates in the write stream's buffer in memory. As part of the
       * pipeline it stays backpressured.
       *
       * Progress is reported as it goes because this is gigabytes and takes minutes — without it
       * the step looks hung.
       */
      const meter = new Transform({
         transform(chunk: Buffer, _enc, cb) {
            hash.update(chunk);
            size += chunk.length;
            if (Date.now() - lastReport > 5000) {
               lastReport = Date.now();
               onLog?.(`  exported ${humanSize(size)}...\n`);
            }
            cb(null, chunk);
         },
      });

      try {
         // pipeline() propagates errors from every stage and destroys the rest on failure, so a
         // disk-full or permission error surfaces instead of being swallowed by an unhandled
         // 'error' event on the write stream. It also settles on the WRITE side finishing, rather
         // than racing the child's exit against the stream flush.
         await pipeline(proc.stdout, meter, this.fs.createWriteStream(tmpFile));
      } catch (e) {
         this.safeUnlink(tmpFile);
         proc.kill("SIGKILL");
         throw new Error(`Writing the image export failed: ${(e as Error).message}. Is there room for ${humanSize(size)}+ in ${os.tmpdir()}?`);
      }

      // The stream ending does not mean docker succeeded — a failure part-way still closes stdout,
      // leaving a truncated tar. Check the exit status before trusting the bytes.
      const code = await new Promise<number | null>((resolve, reject) => {
         proc.on("error", reject);
         if (proc.exitCode !== null) resolve(proc.exitCode);
         else proc.on("close", resolve);
      });
      if (code !== 0) {
         this.safeUnlink(tmpFile);
         throw new Error(`docker save exited with code ${code}: ${stderr.trim() || "(no output)"}`);
      }
      if (size === 0) {
         this.safeUnlink(tmpFile);
         throw new Error(`docker save produced no output for ${imageId}`);
      }

      const tarSha256 = hash.digest("hex");
      // The artifact id IS the content hash, so re-exporting an unchanged image is idempotent
      // and a replica can cache by id.
      const artifact = tarSha256.slice(0, 32);
      const finalPath = this.artifactPath(artifact);

      if (this.fs.existsSync(finalPath)) {
         // Identical content already staged — drop the duplicate rather than overwrite a file
         // that may be mid-download.
         this.safeUnlink(tmpFile);
      } else {
         this.fs.renameSync(tmpFile, finalPath);
      }

      onLog?.(`Exported ${humanSize(size)}, sha256 ${tarSha256.slice(0, 16)}… (artifact ${artifact})\n`);
      this.logger.log(`Staged image artifact ${artifact} for ${imageId} (${humanSize(size)})`);

      return { artifact, imageId, tarSha256, size };
   }

   /**
    * Stream a staged artifact to a replica, encrypted with the same leading-IV AES-256-CTR
    * scheme as the file and database endpoints, so the replica can reuse its existing
    * decryption path unchanged.
    */
   public stream(artifact: string, res: Response): void {
      // Defence in depth: the id is used to build a filesystem path, so reject anything that
      // is not the hex hash prefix we generate. Blocks traversal outright.
      if (!/^[a-f0-9]{32}$/.test(artifact)) {
         this.logger.warn(`Rejected malformed image artifact id: ${artifact}`);
         res.status(400).end();
         return;
      }

      const file = this.artifactPath(artifact);
      if (!this.fs.existsSync(file)) {
         this.logger.warn(`Image artifact not found: ${artifact}`);
         res.status(404).end();
         return;
      }

      res.setHeader("Content-Type", "application/octet-stream");
      res.setHeader("Content-Disposition", `attachment; filename="${artifact}.tar.enc"`);

      const iv = randomBytes(16);
      const cipher = createCipheriv("aes-256-ctr", this.encryptionKey, iv);
      res.write(iv);

      const fileStream = this.fs.createReadStream(file);
      fileStream.pipe(cipher).pipe(res);

      fileStream.on("error", (err) => {
         this.logger.error(`Image artifact read error for ${artifact}: ${err.message}`);
         res.end();
      });
      res.on("finish", () => this.logger.log(`Streamed image artifact ${artifact}`));
   }

   /** Deletes staged artifacts older than the TTL. Best-effort; never throws. */
   public pruneOld(): void {
      try {
         const dir = os.tmpdir();
         for (const entry of this.fs.readdirSync(dir)) {
            const name = entry.name;
            if (!name.startsWith(ARTIFACT_PREFIX)) continue;
            const full = path.join(dir, name);
            try {
               if (Date.now() - this.fs.statSync(full).mtimeMs > ARTIFACT_TTL_MS) {
                  this.fs.unlinkSync(full);
                  this.logger.log(`Pruned stale image artifact ${name}`);
               }
            } catch { /* raced with another prune or an in-flight download; skip */ }
         }
      } catch (e) {
         this.logger.debug(`Artifact prune skipped: ${(e as Error).message}`);
      }
   }

   private artifactPath(artifact: string): string {
      return path.join(os.tmpdir(), `${ARTIFACT_PREFIX}${artifact}.tar`);
   }

   private safeUnlink(file: string): void {
      try {
         if (this.fs.existsSync(file)) this.fs.unlinkSync(file);
      } catch { /* best-effort */ }
   }

   /**
    * Same derivation as ReplicationService: SHA-256 of the configured salt, because AES-256
    * needs exactly 32 bytes and the salt is an arbitrary-length string. Deliberately identical
    * so replicas decrypt images with the key they already hold.
    */
   private get encryptionKey(): Buffer {
      const salt = this.config.get("this-service.password-vault-salt", { infer: true });
      return createHash("sha256").update(salt).digest();
   }

}
