import crypto from "crypto";
import { HttpException, Inject, Injectable, Optional } from "@nestjs/common";
import { AuthService } from "./../auth/auth.service";
import path from "path";
import { UploadedFile } from "./../models/uploadedFile";
import { TempUrl } from "./../models/tempUrl";
import sharp from "sharp";
import ThumbnailGenerator from "fs-thumbnail";
import { REDIS_CACHE, SoftException } from "./../util";
import { FileAccessStat } from "./../models/stats/fileAccessStat";
import { UsedData } from "./../user-profile/user-profile-types";
import { DirectoriesService } from "./../directories/directories.service";
import { AppLogger } from "../logging";
import mime from "mime-types";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { SearchStat } from "./../models/stats/searchStat";
import { ThumbnailCacheInterceptor } from "./thumbnail-cache.interceptor";
import { AbstractFileSystem } from "src/file-system/abstract-file-system.interface";
import { TieredStorageService } from "src/file-system/tiered-storage.service";
import { ConfigService } from "@nestjs/config";
import { EnvVariables, ReplicationRole } from "src/config/config.validator";
import type Redis from "ioredis";
import { minimatch } from "minimatch";
import { BackupLocation, FileBackups } from "./filesApiTypes";
import { ReplicaLinkRegistry, type ReplicaFileResult } from "src/replication/replica-link.registry";
import { FeatureFlagService } from "src/admin/feature-flag.service";
import { FeatureFlagNamespace } from "src/models/admin/featureFlag";
import { fromBuffer as pdfToImage } from "pdf2pic";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { MetricsPusherService, MetricUnit } from "../metrics-pusher.service";
import { User } from "src/models/user";
import { execFile } from "child_process";
import { promisify } from "util";
import { ReplicationService } from "src/replication/replication.service";

type FileServiceResult = Promise<[boolean, string]>;

@Injectable()
export class FilesService {
   public static readonly METADATA_FOLDER_NAME = ".metadata";
   public static readonly THUMBNAILS_FOLDER_NAME = ".thumbnails";
   private readonly dirService: DirectoriesService; // Not injected, because it would cause a circular dependency

   // Short-lived cache of resolved user root paths. getUserRootPath() is called several
   // times per file request (via info/absolutePath/isOwner/updateStats), and each call
   // otherwise costs a DB lookup + a cross-service RPC to auth-api (see getEmail). A
   // user's email→root mapping is effectively immutable, so memoize it briefly to
   // collapse those repeated round-trips on hot paths like audio streaming.
   private readonly rootPathCache = new Map<number, { path: string; expires: number }>();
   private static readonly ROOT_PATH_TTL_MS = 5 * 60 * 1000;

   constructor(
      private readonly userService: AuthService,
      @InjectRepository(UploadedFile) private readonly uploadedFileRepo: Repository<UploadedFile>,
      @InjectRepository(SearchStat) searchStateRepo: Repository<SearchStat>,
      @InjectRepository(FileAccessStat) private readonly fileAccessStatRepo: Repository<FileAccessStat>,
      @InjectRepository(TempUrl) private readonly tempUrlRepo: Repository<TempUrl>,
      @InjectRepository(User) private readonly userRepo: Repository<User>,
      @Inject() private readonly logger: AppLogger,
      @Inject(REDIS_CACHE) private readonly cache: Redis,
      @Inject() private readonly fs: AbstractFileSystem,
      @Inject() private readonly config: ConfigService<EnvVariables>,
      @Inject() private readonly featureFlagService: FeatureFlagService,
      @Inject() private readonly tieredStorage: TieredStorageService,
      @Optional() @Inject(MetricsPusherService) private readonly metrics?: MetricsPusherService,
      @Optional() @Inject(ReplicaLinkRegistry) private readonly replicaLink?: ReplicaLinkRegistry,
   ) {
      this.dirService = new DirectoriesService(
         userService,
         this,
         uploadedFileRepo,
         searchStateRepo,
         logger,
         fs,
         config,
         this.tieredStorage,
      );

      // Sharp cache
      sharp.cache(true);
      sharp.cache({ memory: 256, items: 5000, files: 500 });
      sharp.simd(true);

      // Investigation gauges for the memory-leak hunt. These are sampled live on each
      // metrics flush (see MetricsPusherService.registerGauge):
      //  - chunked_upload_sessions / _bytes_pending: a steady climb means chunked-upload
      //    sessions are being abandoned/leaked in memory (the path we hardened).
      //  - sharp_cache_*: the libvips cache footprint, so we can see if it's a contributor.
      if (this.metrics) {
         this.metrics.registerGauge("chunked_upload_sessions", MetricUnit.Count, () => this.chunkedUploads.size);
         this.metrics.registerGauge("chunked_upload_bytes_pending", MetricUnit.Bytes, () => {
            let sum = 0;
            for (const u of this.chunkedUploads.values()) sum += u.totalSize;
            return sum;
         });
         this.metrics.registerGauge("sharp_cache_memory_bytes", MetricUnit.Bytes, () => {
            const stats = sharp.cache() as unknown as { memory?: { current?: number } };
            return Math.round((stats.memory?.current ?? 0) * 1024 * 1024); // sharp reports MB
         });
         this.metrics.registerGauge("sharp_cache_items", MetricUnit.Count, () => {
            const stats = sharp.cache() as unknown as { items?: { current?: number } };
            return stats.items?.current ?? 0;
         });
      }
   }

   public async asStream(userId: number, relativePath: string, user_agent: string, options?: any) {
      const dir = await this.absolutePath(userId, relativePath);
      if (!this.fs.existsSync(dir)) throw new Error(dir + " does not exist");

      // Verify ownership BEFORE doing any work (including stat writes). Otherwise a denied
      // traversal request would still persist a spurious UploadedFile + FileAccessStat row
      // (under this user's id, with a "../..." path) via updateStats().
      const owns = await this.isOwner(userId, dir);
      if (!owns) {
         throw new Error("You don't have permission to access this file " + relativePath);
      }

      this.updateStats(userId, dir, user_agent).catch((e) =>
         this.logger.error("updateStats failed: " + (e as Error).message),
      );

      return this.fs.createReadStream(dir, options);
   }

   public async upload(userId: number, file: Express.Multer.File, dest: string): FileServiceResult {
      try {
         // Check if user has enough space to upload the file
         const usedData = await this.getUsedData(userId);
         const user = await this.userService.getById(userId);

         if (usedData.total() + file.size > user.getMaxData()) {
            return [false, "You don't have enough space to upload this file"];
         }

         const cleanName = path.join(dest, this.replaceIllegalChars(file.originalname));
         const root = await this.getUserRootPath(userId);
         const dir = await this.absolutePath(userId, cleanName);

         const owns = await this.isOwner(userId, dir);
         if (!owns) {
            return [false, "You don't have permission to upload here"];
         }

         const relative = path.relative(root, dir);

         this.fs.writeFileSync(dir, file.buffer);

         let fileDB = await this.uploadedFileRepo.findOne({
            where: { absolute_path: relative, user: { id: userId } },
         });

         // if a file already exists with that name, then most likely we are replacing a file
         // in this case, we'll invalidate old thumbnails
         if (fileDB) {
            fileDB.updated_at = new Date();
         } else {
            fileDB = new UploadedFile();
            fileDB.absolute_path = relative;
            fileDB.user = await this.userService.getById(userId);
            fileDB.mime = file.mimetype;
         }
         await this.invalidateThumbnailsFor(userId, fileDB);
         await this.uploadedFileRepo.save(fileDB);

         // Update cache for used data
         usedData.other += file.size;
         await this.cache.setex(`${userId}::used_data`, 3600, JSON.stringify(usedData));

         return [true, ""];
      } catch (e) {
         return [false, (e as Error).message];
      }
   }

   private chunkedUploads = new Map<string, { userId: number; dest: string; filename: string; totalSize: number; dir: string; received: Set<number> }>();

   public async chunkedUploadInit(userId: number, dest: string, filename: string, totalSize: number) {
      const usedData = await this.getUsedData(userId);
      const user = await this.userService.getById(userId);
      if (usedData.total() + totalSize > user.getMaxData()) {
         throw new Error("You don't have enough space to upload this file");
      }

      const cleanName = path.join(dest, this.replaceIllegalChars(filename));
      const dir = await this.absolutePath(userId, cleanName);
      if (!(await this.isOwner(userId, dir))) {
         throw new Error("You don't have permission to upload here");
      }

      const uploadId = crypto.randomUUID();
      const tmpDir = dir + ".chunked_tmp";
      this.fs.mkdirSync(tmpDir, { recursive: true });
      this.chunkedUploads.set(uploadId, { userId, dest, filename, totalSize, dir, received: new Set() });
      return { uploadId };
   }

   public async chunkedUploadPart(userId: number, uploadId: string, index: number, file: Express.Multer.File) {
      const upload = this.chunkedUploads.get(uploadId);
      if (!upload || upload.userId !== userId) throw new Error("Invalid upload session");

      const tmpDir = upload.dir + ".chunked_tmp";
      this.fs.writeFileSync(path.join(tmpDir, `part_${index}`), file.buffer);
      upload.received.add(index);
   }

   public async chunkedUploadComplete(userId: number, uploadId: string) {
      const upload = this.chunkedUploads.get(uploadId);
      if (!upload || upload.userId !== userId) throw new Error("Invalid upload session");

      const tmpDir = upload.dir + ".chunked_tmp";
      const parts = Array.from(upload.received).sort((a, b) => a - b);

      try {
         // Stream each part directly into the destination file in order. Memory stays
         // bounded to the stream highWaterMark (~64 KB) regardless of file size, instead
         // of loading the entire (multi-GB) file into Buffers via Buffer.concat — which
         // was the off-heap arrayBuffers leak on the chunked-upload (>100 MB) path.
         const dest = this.fs.createWriteStream(upload.dir);
         try {
            for (let i = 0; i < parts.length; i++) {
               const partPath = path.join(tmpDir, `part_${parts[i]}`);
               const isLast = i === parts.length - 1;
               // Keep `dest` open across parts; only end it after the final chunk.
               await pipeline(this.fs.createReadStream(partPath), dest, { end: isLast });
               this.fs.unlinkSync(partPath);
            }
         } catch (e) {
            dest.destroy();
            throw e;
         }

         // Clean up temp dir
         try { this.fs.rmdirSync(tmpDir, { recursive: true }); } catch (e) { this.logger.logException(e); }

         // Save to DB (same logic as regular upload)
         const root = await this.getUserRootPath(userId);
         const relative = path.relative(root, upload.dir);
         let fileDB = await this.uploadedFileRepo.findOne({
            where: { absolute_path: relative, user: { id: userId } },
         });
         if (fileDB) {
            await this.invalidateThumbnailsFor(userId, fileDB);
         } else {
            fileDB = new UploadedFile();
            fileDB.absolute_path = relative;
            fileDB.user = await this.userService.getById(userId);
            fileDB.mime = mime.lookup(upload.filename) || "application/octet-stream";
            await this.uploadedFileRepo.save(fileDB);
         }
      } finally {
         // Always release the in-memory session — even on failure — so the Map can't
         // grow from abandoned/failed completions.
         this.chunkedUploads.delete(uploadId);
      }
   }

   public async new(userId: number, name: string): Promise<void> | never {
      const root = await this.getUserRootPath(userId);
      const dir = path.join(root, name);
      const relative = path.relative(root, dir);

      if (!(await this.isOwner(userId, dir))) {
         throw new Error("You don't have permission to create files here");
      }

      this.verifyFileName(dir);

      this.fs.writeFileSync(dir, "");

      // Register file in DB
      const file = new UploadedFile();
      file.user = await this.userService.getById(userId);
      file.absolute_path = relative;
      file.mime = "text/plain";
      await this.uploadedFileRepo.save(file);
   }

   public async save(
      userId: number,
      fileRelativePath: string,
      content: string,
      append: boolean | string = false,
   ): FileServiceResult {
      try {
         const dir = await this.absolutePath(userId, fileRelativePath);
         const owns = await this.isOwner(userId, dir);
         if (!owns) {
            return [false, "You don't have permission to save here"];
         }

         if (!append || append == "false") {
            this.fs.writeFileSync(dir, content);
         } else {
            this.fs.appendFileSync(dir, content);
         }

         // Update the DB record if it exists
         const relative = path.normalize(fileRelativePath);
         let file = await this.uploadedFileRepo.findOne({
            where: { absolute_path: relative, user: { id: userId } },
         });
         if (file) {
            this.logger.debug(`[::${this.save.name}] found file ${file.id} (${fileRelativePath}) in DB. Updated updated_at field`);
            file.updated_at = new Date();
         } else {
            this.logger.debug(`[::${this.save.name}] no match for ${fileRelativePath} in DB (searched ${relative} in DB for ${userId})`);
            
            file = new UploadedFile();
            file.absolute_path = relative;
            file.user = await this.userService.getById(userId);
            file.mime = mime.lookup(path.basename(dir)) || "application/octet-stream";
         }
         await this.uploadedFileRepo.save(file);

         return [true, ""];
      } catch (e) {
         return [false, (e as Error).message];
      }
   }

   public async delete(userId: number, relativePath: string): FileServiceResult {
      try {
         const root = await this.getUserRootPath(userId);
         const dir = await this.absolutePath(userId, relativePath);

         if (!(await this.isOwner(userId, dir))) {
            return [false, "You don't have permission to delete this file"];
         }

         const relative = path.relative(root, dir);
         // If this is a cold file, free its cold blob now (don't wait for GC) — otherwise a
         // re-upload of the same name before GC would leave a stale blob at the mirror path.
         await this.tieredStorage.removeColdData(dir);
         this.fs.unlinkSync(dir);

         // See if file is in DB, if yes, then delete it
         const user = await this.userService.getById(userId);
         const uploadedFile = await this.uploadedFileRepo.findOne({
            where: { absolute_path: relative, user: { id: user.id } },
         });
         const accessData = await this.fileAccessStatRepo.find({
            where: { uploaded_file: uploadedFile },
         });
         if (accessData) await this.fileAccessStatRepo.softRemove(accessData);

         // Delete all thumbnails relate to that file
         if (uploadedFile) {
            await this.invalidateThumbnailsFor(userId, uploadedFile);
            await this.uploadedFileRepo.softRemove(uploadedFile);
         }

         return [true, ""];
      } catch (e) {
         return [false, (e as Error).message];
      }
   }

   public async rename(userId: number, name: string, newName: string): Promise<void> | never {
      const root = await this.getUserRootPath(userId);
      const dir = await this.absolutePath(userId, name);
      const newDir = await this.absolutePath(userId, newName);
      const relative = path.relative(root, dir);
      const relativeNew = path.relative(root, newDir);

      if (!(await this.isOwner(userId, dir))) {
         throw new Error("You don't have permission to rename this file");
      }

      this.verifyFileName(newDir);

      this.fs.renameSync(dir, newDir);

      // Rename file in DB
      const file = await this.uploadedFileRepo.findOne({
         where: { absolute_path: relative, user: { id: userId } },
      });

      if (file) {
         this.logger.debug(`[::${this.rename.name}] Renaming file in DB from ${relative} to ${relativeNew}`);
         file.absolute_path = relativeNew;
         await this.uploadedFileRepo.save(file);
      } else {
         // Else if it is not in DB then insert it
         this.logger.debug(
            `[::${this.rename.name}] File found found in DB. Inserting it from ${relative} to ${relativeNew}`,
         );

         const mime = FilesService.detectFile(newDir);
         const user = await this.userService.getById(userId);

         const uploadedFile = new UploadedFile();
         uploadedFile.user = user;
         uploadedFile.absolute_path = relativeNew;
         uploadedFile.mime = mime;
         await this.uploadedFileRepo.save(uploadedFile);
      }
   }

   public async info(userId: number, relativePath: string, fetch_related_keys_in_redis = false, fetch_db_records = false) {
      const root = await this.getUserRootPath(userId);
      const dir = await this.absolutePath(userId, relativePath);
      const relative = path.relative(root, dir);

      if (!(await this.isOwner(userId, dir))) {
         throw new Error("You don't have permission to access this file");
      }

      const stats = this.fs.statSync(dir);

      // For directories, summarise how many files (recursively) are in cold storage.
      let file_count: number | undefined;
      let cold_file_count: number | undefined;
      // A directory inode has no content size — statSync(dir).size is just the size of
      // the directory entry itself (~4 KB block on ext4), which is why every folder
      // showed "4 KB". Delegate to the OS `du` for the real total (a single native call).
      let size = stats.size;
      if (stats.isDirectory()) {
         const s = this.tieredStorage.coldStats(dir);
         file_count = s.total;
         cold_file_count = s.cold;
         size = await this.getDirectorySize(dir);
      }

      const file = await this.uploadedFileRepo.findOne({
         where: { absolute_path: relative, user: { id: userId } },
      });

      const fileMime = file ? file.mime : FilesService.detectFile(dir);

      // Get temp url if exists and is active
      const tempUrls = await this.tempUrlRepo.find({
         where: { user: { id: userId }, filepath: relative },
      });

      // Get all cached thumbnails for this file
      const thumbails: string[] = [];
      if (file && fetch_db_records) {
         const thumbnailFolder = path.join(
            await this.createMetaFolderIfNotExists(userId),
            FilesService.THUMBNAILS_FOLDER_NAME,
         );
         const files = this.fs.readdirSync(thumbnailFolder);
         files.forEach((fileEntry) => {
            if (fileEntry.name.startsWith(`${file.id}_`)) {
               thumbails.push(fileEntry.name);
            }
         });
      }

      return {
         extension: path.extname(relativePath),
         mime: fileMime,
         path: path.relative(await this.getUserRootPath(userId), dir),
         name: path.basename(dir),
         is_image: fileMime.includes("image"),
         is_text: fileMime.includes("text") || fileMime == "application/x-empty",
         is_video: fileMime.includes("video"),
         is_audio: fileMime.includes("audio"),
         is_pdf: fileMime.includes("pdf"),
         size: size,
         lastModified: stats.mtime.toISOString(),
         is_cold_storage: this.tieredStorage.isColdFile(dir),
         file_count,
         cold_file_count,
         temp_url: tempUrls.length > 0 ? tempUrls.filter((e) => e.isValid()) : null,
         db_record: file,
         related_keys_in_redis: file && fetch_related_keys_in_redis ? await this.getCacheKeysForFile(userId, file) : [],
         thumbails,
         is_dir: stats.isDirectory(),
      };
   }

   /**
    * Total byte size of a directory's contents. A directory has no content size in its
    * inode (statSync returns the ~4 KB block of the dir entry itself), so we ask the OS
    * `du` — one native call that walks the tree far faster than a JS recursion. Falls
    * back to the inode size if `du` is unavailable (e.g. non-disk fs) or errors.
    */
   private async getDirectorySize(absPath: string): Promise<number> {
      try {
         // Promisified lazily (not at module load) so an incomplete `child_process`
         // mock in tests can't crash the import; any failure falls back to statSync.
         const execFileAsync = promisify(execFile);
         if (process.platform === "darwin") {
            // BSD `du` (macOS) has no -b; -sk reports total in 1024-byte blocks.
            const { stdout } = await execFileAsync("du", ["-sk", absPath]);
            const kb = parseInt(stdout.trim().split(/\s+/)[0], 10);
            return Number.isFinite(kb) ? kb * 1024 : this.fs.statSync(absPath).size;
         }
         // GNU `du` (Linux): -s summary, -b apparent size in bytes (matches the logical
         // file sizes shown for individual files).
         const { stdout } = await execFileAsync("du", ["-sb", absPath]);
         const bytes = parseInt(stdout.trim().split(/\s+/)[0], 10);
         return Number.isFinite(bytes) ? bytes : this.fs.statSync(absPath).size;
      } catch {
         return this.fs.statSync(absPath).size;
      }
   }

   public async exists(userId: number, relativePath: string) {
      const dir = await this.absolutePath(userId, relativePath);

      if (!(await this.isOwner(userId, dir))) {
         throw new Error("You don't have permission to access this file");
      }

      return this.fs.existsSync(dir);
   }

   /**
    * Minimal file metadata needed to serve a byte stream: size + mime and the
    * video/audio flags used to decide Range handling. Deliberately leaner than
    * info() — it skips the temp-url lookup, thumbnail enumeration and redis-key
    * expansion that the streaming hot path never uses, shaving a DB query off
    * every song start/skip.
    */
   public async streamInfo(userId: number, relativePath: string): Promise<{ mime: string; size: number; is_video: boolean; is_audio: boolean }> {
      const root = await this.getUserRootPath(userId);
      const dir = await this.absolutePath(userId, relativePath);
      if (!(await this.isOwner(userId, dir))) {
         throw new Error("You don't have permission to access this file");
      }

      const stats = this.fs.statSync(dir);
      const relative = path.relative(root, dir);
      const file = await this.uploadedFileRepo.findOne({
         where: { absolute_path: relative, user: { id: userId } },
      });
      const mime = file ? file.mime : FilesService.detectFile(dir);

      return {
         mime,
         size: stats.size,
         is_video: mime.includes("video"),
         is_audio: mime.includes("audio"),
      };
   }

   public async toThumbnail(
      path_: string,
      userId: number,
      width: number | undefined = undefined,
      height: number | undefined = undefined,
   ) {
      const dir = await this.absolutePath(userId, path_);
      const fileMime = FilesService.detectFile(dir);

      if (!(await this.isOwner(userId, dir))) {
         throw new Error("You don't have permission to access this file");
      }

      if (fileMime.includes("image")) {
         if (!this.fs.existsSync(dir)) throw new Error(dir + " does not exist");

         // If raw thumbnails flag is enabled, skip resizing and return the original file
         if (await this.featureFlagService.isFeatureFlagEnabled(FeatureFlagNamespace.Files, "disable_thumbnail_resizing_with_sharp")) {
            return this.fs.createReadStream(dir);
         }

         // Check if thumbnail already exists
         const uploadedFile = await this.uploadedFileRepo.findOne({
            where: { absolute_path: path.normalize(path_), user: { id: userId } },
         });
         const thumbnailFolder = path.join(
            await this.createMetaFolderIfNotExists(userId),
            FilesService.THUMBNAILS_FOLDER_NAME,
         );
         if (
            (await this.featureFlagService.isFeatureFlagDisabled(
               FeatureFlagNamespace.Files,
               "disable_thumbnail_caching_disk",
            )) &&
            uploadedFile
         ) {
            const thumbnailPath = path.join(
               thumbnailFolder,
               `${uploadedFile.id}_${width}x${height}${path.extname(path_)}`,
            );

            if (this.fs.existsSync(thumbnailPath)) {
               this.logger.debug(`[${this.toThumbnail.name}] Found cached thumbnail at ${thumbnailPath}`);
               return this.fs.createReadStream(thumbnailPath);
            }
         }

         const resized = sharp()
            .resize(Number(width) || undefined, Number(height) || undefined)
            .withMetadata();
         const readStream = (this.fs.createReadStream(dir)).pipe(resized);

         // cache thumbnail for next time and return it
         // Don't do it if we are inside the thumbnail folder (to avoid recursive thumbnail generation)
         if (
            (await this.featureFlagService.isFeatureFlagDisabled(
               FeatureFlagNamespace.Files,
               "disable_thumbnail_caching_disk",
            )) &&
            uploadedFile &&
            !path.normalize(dir).includes(path.normalize(FilesService.THUMBNAILS_FOLDER_NAME))
         ) {
            const thumbnailPath = path.join(
               thumbnailFolder,
               `${uploadedFile.id}_${width}x${height}${path.extname(path_)}`,
            );
            // Resize fully in memory, then persist with a synchronous write. Never toFile() here:
            // libvips creates the target up front and fills it in 8 KB chunks from a worker thread,
            // so the cache check above (existsSync) let a concurrent request for the same size
            // stream a still-empty/truncated file — which ThumbnailCacheInterceptor then pinned in
            // Redis for 30 days. writeFileSync completes before any other request on this process
            // can run, so the cached file is never observable half-written.
            const buffer = await readStream.toBuffer();
            this.fs.writeFileSync(thumbnailPath, buffer);

            this.logger.debug(`[${this.toThumbnail.name}] Created cached thumbnail at ${thumbnailPath}`);
            return Readable.from(buffer);
         }

         if (!uploadedFile)
            this.logger.debug(
               `[${this.toThumbnail.name}] Unable to cache thumbnail for ${path_} because it is not indexed`,
            );

         this.logger.debug(`[${this.toThumbnail.name}] Returning computed resized thumbnail for ${path_}`);

         return readStream;
      } else if (fileMime.includes("video")) {
         // If it is a video generate thumbnail
         const thumbnailPath = path.join(path.dirname(dir), ".videometa." + path.basename(dir) + ".png");

         this.logger.debug(`[${this.toThumbnail.name}] Generating thumbnail for video at ${path_}`);

         // TOOD: CHange this library because it is hard to test
         // instead of returning errors it prints them to the console
         const thumbGen = new ThumbnailGenerator({
            verbose: false, // Whether to print out warning/errors
            size: [width ?? "?", height ?? "?"], // Default size, either a single number of an array of two numbers - [width, height].
            quality: 70, // Default quality, between 1 and 100
         });

         await thumbGen.getThumbnail({
            path: dir,
            output: thumbnailPath,
         });

         // Delete that thumbnail after 1 second (request sent)
         // TODO make this a job instead
         setTimeout(() => {
            try {
               this.fs.unlinkSync(thumbnailPath);
            } catch (e) {
               this.logger.error(`Failed to delete temp thumbnail ${thumbnailPath}: ${(e as Error).message}`);
            }
         }, 1000);

         return this.fs.createReadStream(thumbnailPath);
      } else if (fileMime.includes("pdf")) {
         // Generate thumbnail for PDF first page
         if (!this.fs.existsSync(dir)) throw new Error(dir + " does not exist");

         const uploadedFile = await this.uploadedFileRepo.findOne({
            where: { absolute_path: path.normalize(path_), user: { id: userId } },
         });
         const thumbnailFolder = path.join(
            await this.createMetaFolderIfNotExists(userId),
            FilesService.THUMBNAILS_FOLDER_NAME,
         );

         // Check if cached thumbnail exists
         if (
            (await this.featureFlagService.isFeatureFlagDisabled(
               FeatureFlagNamespace.Files,
               "disable_thumbnail_caching_disk",
            )) &&
            uploadedFile
         ) {
            const thumbnailPath = path.join(thumbnailFolder, `${uploadedFile.id}_pdf_${width}x${height}.png`);
            if (this.fs.existsSync(thumbnailPath)) {
               this.logger.debug(`[${this.toThumbnail.name}] Found cached PDF thumbnail at ${thumbnailPath}`);
               return this.fs.createReadStream(thumbnailPath);
            }
         }

         this.logger.debug(`[${this.toThumbnail.name}] Generating thumbnail for PDF at ${path_}`);

         const targetWidth = Number(width) || 400;
         const targetHeight = Number(height) || 300;
         const pageNumber = 1;

         // Read PDF file
         const pdfBuffer = Buffer.from(this.fs.readFileSync(dir, "binary") as string, "binary");
         const tempFilename = `pdf_thumb_${Date.now()}`;
         const options = {
            density: 150,
            format: "png",
            width: targetWidth,
            savePath: thumbnailFolder,
            saveFilename: tempFilename,
         };

         const convert = pdfToImage(pdfBuffer, options);
         const result = await convert(pageNumber, { responseType: "buffer" });

         // Clean up temp file created by pdf2pic
         const tempPath = path.join(thumbnailFolder, `${tempFilename}.${pageNumber}.png`);
         if (this.fs.existsSync(tempPath)) this.fs.unlinkSync(tempPath);

         if (result.buffer) {
            // Crop to top portion and resize in memory
            const metadata = await sharp(result.buffer).metadata();
            const cropHeight = Math.min(metadata.height || targetHeight, Math.floor((metadata.height || targetHeight) * 0.5));
            const croppedBuffer = await sharp(result.buffer)
               .extract({ left: 0, top: 0, width: metadata.width || targetWidth, height: cropHeight })
               .resize(targetWidth, targetHeight, { fit: "cover" })
               .png()
               .toBuffer();

            // If caching is enabled, save to disk
            if (
               (await this.featureFlagService.isFeatureFlagDisabled(
                  FeatureFlagNamespace.Files,
                  "disable_thumbnail_caching_disk",
               )) &&
               uploadedFile
            ) {
               const cachedPath = path.join(thumbnailFolder, `${uploadedFile.id}_pdf_${width}x${height}.png`);
               this.fs.writeFileSync(cachedPath, croppedBuffer);
               this.logger.debug(`[${this.toThumbnail.name}] Cached PDF thumbnail at ${cachedPath}`);
               return this.fs.createReadStream(cachedPath);
            }

            // Return buffer as stream
            return Readable.from(croppedBuffer);
         }

         return null;
      }
      return null;
   }

   public async getUserRootPath(userId: number): Promise<string> {
      const cached = this.rootPathCache.get(userId);
      if (cached && cached.expires > Date.now()) return cached.path;

      const email = await this.userService.getEmail(userId);
      if (!email) {
         throw new HttpException(
            {
               errors: [{ field: "", message: "Invalid user Id" }],
            },
            400,
         );
      }

      const dir = path.join(this.config.get("this-service.cloud-dir", { infer: true }), email);
      // Lazily create user directory on first access (e.g. after registering via auth API)
      if (!this.fs.existsSync(dir)) {
         this.fs.mkdirSync(dir, { recursive: true });
      }
      this.rootPathCache.set(userId, { path: dir, expires: Date.now() + FilesService.ROOT_PATH_TTL_MS });
      return dir;
   }

   public async absolutePath(userId: number, relativePath: string) {
      return path.join(await this.getUserRootPath(userId), relativePath);
   }

   /**
    * Backup / redundancy report for a single file. Answers "how many copies of this
    * file exist and where":
    *  1. the primary copy in cloud-dir (this node),
    *  2. any locally configured mirror disks (this-service.replication.mirror-dirs) — full copies
    *     of the cloud-dir tree on other disks, so the file's mirror copy lives at
    *     <mirror-dir>/<path-relative-to-cloud-dir>,
    *  3. each replica registered with the master (from the Redis registry) — presence
    *     is inferred from the replica's last sync time vs the file's mtime plus the
    *     replication ignore rules (the master never verifies a replica's disk directly).
    */
   public async getBackups(userId: number, relativePath: string): Promise<FileBackups> {
      const absolute = await this.absolutePath(userId, relativePath);
      if (!(await this.isOwner(userId, absolute))) {
         throw new SoftException("You don't have access to this file");
      }

      // Replica IPs are infrastructure detail — only expose them to admins. Fail closed
      // (redact) if the admin check is unavailable.
      let isAdminUser = false;
      try {
         const user = await this.userService.getById(userId);
         if (user) isAdminUser = await this.userService.isAdmin(user.shadoUserId);
      } catch {
         isAdminUser = false;
      }

      const cloudDir = this.config.get("this-service.cloud-dir", { infer: true });
      // Path relative to the cloud-dir root — the key shared by mirror disks and replicas,
      // which all mirror the entire cloud-dir tree.
      const relToCloud = path.relative(cloudDir, absolute);

      const locations: BackupLocation[] = [];

      // 1) Primary copy (cloud-dir on this node)
      //
      // A cold-tiered file IS a symlink to the cold drive, and existsSync follows it — so when that
      // drive is unmounted the file would be reported as "Missing from primary storage", which is
      // both alarming and wrong: the bytes exist, the drive is just not attached. Distinguish
      // unreachable from absent the same way the mirror-disk branch below already does, by checking
      // for the entry itself with lstat (which does not follow).
      const primaryReachable = this.fs.existsSync(absolute);
      let primaryEntryPresent = primaryReachable;
      if (!primaryReachable) {
         try {
            this.fs.lstatSync(absolute);
            primaryEntryPresent = true;
         } catch {
            primaryEntryPresent = false;
         }
      }

      if (primaryReachable) {
         locations.push({ kind: "primary", label: "Cloud storage", present: true, detail: "Primary copy" });
      } else if (primaryEntryPresent) {
         locations.push({
            kind: "primary",
            label: "Cloud storage",
            present: null,
            detail: "In cold storage — drive not mounted / unavailable",
         });
      } else {
         locations.push({ kind: "primary", label: "Cloud storage", present: false, detail: "Missing from primary storage" });
      }

      // 2) Local mirror disks (this node's config)
      const mirrorDirs = this.config.get("this-service.replication.mirror-dirs", { infer: true }) ?? [];
      for (const dir of mirrorDirs) {
         // If the mirror root is missing the disk is almost certainly unmounted.
         if (!this.fs.existsSync(dir)) {
            locations.push({ kind: "mirror", label: `Mirror disk (${dir})`, present: null, detail: "Disk not mounted / unavailable" });
            continue;
         }
         const mirrorFile = path.join(dir, relToCloud);
         const exists = this.fs.existsSync(mirrorFile);
         locations.push({
            kind: "mirror",
            label: `Mirror disk (${dir})`,
            present: exists,
            detail: exists ? "Mirrored copy present" : "Not yet mirrored",
         });
      }

      // 3) Replicas — the master queries each connected replica live over the
      // replica-link socket (replicas dial out to the master, so this works even though
      // they're not reachable from the internet). Connected replicas give an
      // authoritative answer for their cloud-dir AND their own mirror disks; replicas
      // that are registered but currently offline can't be verified.
      const ignored = this.isReplicationIgnored(relToCloud);
      const role = this.config.get("this-service.replication.role", { infer: true });
      const isMaster = role === ReplicationRole.Master || role === ReplicationRole.Primary;

      if (isMaster) {
         // Known replicas (Redis registry, keyed by IP + device name) — used to surface
         // replicas that are currently offline.
         let registry: Record<string, string> = {};
         try {
            registry = await this.cache.hgetall(ReplicationService.REPLICAS_KEY);
         } catch {
            /* redis unavailable — skip offline listing */
         }
         const known: { key: string; ip: string; deviceName?: string; lastSeenAt: number }[] = [];
         for (const raw of Object.values(registry ?? {})) {
            try {
               const r = JSON.parse(raw) as { ip: string; deviceName?: string; lastSeenAt: number };
               known.push({ key: `${r.ip}|${r.deviceName ?? ""}`, ip: r.ip, deviceName: r.deviceName, lastSeenAt: r.lastSeenAt });
            } catch {
               /* skip corrupt entry */
            }
         }

         // Live per-file check of every currently-connected replica.
         let live: ReplicaFileResult[] = [];
         try {
            live = (await this.replicaLink?.queryFile(relToCloud)) ?? [];
         } catch {
            live = [];
         }
         // Correlate connected vs offline by IP + device name (not IP alone, since two
         // replicas can share one public IP behind NAT).
         const connectedKeys = new Set(live.map((l) => `${l.ip}|${l.deviceName ?? ""}`));

         // Label: device name for everyone; the IP is admin-only. Falls back to an index
         // when the device name is unknown.
         let replicaIndex = 0;
         const replicaLabel = (deviceName?: string, ip?: string): string => {
            replicaIndex++;
            const base = deviceName && deviceName.trim() ? deviceName : `Replica ${replicaIndex}`;
            return isAdminUser && ip ? `${base} (${ip})` : base;
         };

         for (const res of live) {
            const label = replicaLabel(res.deviceName, res.ip);

            if (!res.report) {
               // Connected but didn't answer the file check in time.
               locations.push({ kind: "replica", label, present: null, detail: "Online, but did not respond to the file check in time" });
               continue;
            }

            locations.push({
               kind: "replica",
               label,
               present: res.report.cloudDir,
               detail: res.report.cloudDir ? "Verified present on replica (live)" : "Not present on replica (verified live)",
            });

            // The replica's own mirror disks (also verified live).
            for (const m of res.report.mirrors) {
               locations.push({
                  kind: "mirror",
                  label: isAdminUser ? `${label} · mirror (${m.dir})` : `${label} · mirror`,
                  present: m.present,
                  detail: m.present ? "Mirrored copy present on replica" : "Not on replica mirror disk",
               });
            }
         }

         // Registered replicas that aren't currently connected — cannot verify.
         for (const k of known) {
            if (connectedKeys.has(k.key)) continue;
            const label = replicaLabel(k.deviceName, k.ip);
            const lastSeen = new Date(k.lastSeenAt).toISOString();
            locations.push({ kind: "replica", label, present: null, detail: `Offline — cannot verify (last seen ${lastSeen})` });
         }
      } else if (role === ReplicationRole.Replica) {
         // This node is itself a replica; its local copy is a backup of the master.
         locations.push({
            kind: "replica",
            label: "Master (source of truth)",
            present: true,
            detail: "This node is a replica of the master",
         });
      }

      const confirmedCopies = locations.filter((l) => l.present === true).length;

      return { path: relToCloud, role: String(role), ignored, confirmedCopies, locations };
   }

   /** True if a cloud-dir-relative path matches any configured replication ignore glob. */
   private isReplicationIgnored(relToCloudPath: string): boolean {
      const patterns = this.config.get("this-service.replication.ignore-patterns", { infer: true });
      if (!patterns || patterns.length === 0) return false;
      const normalized = relToCloudPath.split(path.sep).join("/");
      return patterns.some((p) => minimatch(normalized, p, { dot: true }));
   }

   public static detectFile(filename: string): string {
      const result = mime.lookup(filename);
      // if mime doesn't regonize it, then we'll try to guess it
      if (result == false) {
         if (filename.includes("jpg") || filename.includes("jpeg")) return "image/jpeg";
         if (filename.includes("png")) return "image/png";
      }
      return result == false ? "" : result;
   }

   public verifyFileName(fullpath: string) {
      // Verify that the user is not creating a hidden folder
      const basename = path.basename(fullpath);
      if (basename.startsWith(".")) throw new SoftException("Directory/File name cannot start with '.'");

      // Check for illegal chars
      const illegal = ["?", "!", "[", "]", "{", "}", "/", "\\", "*", "<", ">", "|", '"', "'", ":", "@"];
      for (const c of illegal) {
         if (basename.includes(c)) throw new SoftException("Directory/File name cannot contain " + c);
      }
   }

   public replaceIllegalChars(filename: string) {
      // Verify that the user is not creating a hidden folder
      let basename = path.basename(filename);
      while (basename.startsWith(".")) {
         basename = basename.substring(1);
      }

      // Check for illegal chars
      const illegal = ["?", "!", "[", "]", "{", "}", "/", "\\", "*", "<", ">", "|", '"', "'", "@", ":"];
      for (const c of illegal) {
         basename = basename.replace(new RegExp(`\\${c}`, "g"), "");
      }

      // Check if the name is empty after replacing stuff
      if (basename == "") basename = new Date().toLocaleDateString().replace(":", "-");

      return basename;
   }

   public async profilePictureInfo(userId: number) {
      const dir = await this.absolutePath(userId, FilesService.METADATA_FOLDER_NAME + "/prof");
      return {
         exists: this.fs.existsSync(dir),
         path: path.relative(await this.getUserRootPath(userId), dir),
      };
   }

   public async getUsedData(userId: number) {
      const CACHE_KEY = `${userId}::used_data`;
      if (await this.cache.exists(CACHE_KEY)) {
         const value = await this.cache.get(CACHE_KEY);
         try {
            // JSON.parse returns a plain object; rehydrate into a UsedData
            // instance so callers can use methods like total().
            return Object.assign(new UsedData(), JSON.parse(value));
         } catch(e) {
            this.logger.error(`Failed to getUsedData from redis cache (value: ${value}). ` + (e as Error).message);
         }
      }

      const root = await this.getUserRootPath(userId);
      const user = await this.userService.getById(userId);
      const used_data: UsedData = new UsedData();
      used_data.max = user.getMaxData();

      const arrayOfFiles = await this.dirService.listrecursive(userId);
      for (const relativePath of arrayOfFiles) {
         const filePath = path.join(root, relativePath);
         // Get the file extension
         const ext = path.extname(filePath).toLowerCase();
         const size = (this.fs.statSync(filePath)).size;

         if (ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".gif") used_data.images += size;
         else if (ext == ".mp4" || ext == ".webm" || ext == ".mkv" || ext == ".avi" || ext == ".mov" || ext == ".wmv") {
            used_data.videos += size;
         } else if (
            ext == ".pdf" ||
            ext == ".doc" ||
            ext == ".docx" ||
            ext == ".xls" ||
            ext == ".xlsx" ||
            ext == ".ppt" ||
            ext == ".pptx" ||
            ext == ".odt" ||
            ext == ".ods" ||
            ext == ".odp" ||
            ext == ".txt" ||
            ext == ".rtf"
         ) {
            used_data.documents += size;
         } else used_data.other += size;
      }

      // Cache it in redis (fire-and-forget; not critical to the returned value)
      void this.cache.setex(CACHE_KEY, 3600, JSON.stringify(used_data));

      return used_data;
   }

   /** Counts how many of the user's files (recursively) are in cold/tiered storage. */
   public async getColdStorageStats(userId: number): Promise<{ total: number; cold: number }> {
      const root = await this.getUserRootPath(userId);
      const cold = this.tieredStorage.coldStats(root);
      return { total: cold.total, cold: cold.cold };
   }

   public async createMetaFolderIfNotExists(userId: number): Promise<string> {
      const dir = await this.absolutePath(userId, FilesService.METADATA_FOLDER_NAME);
      if (!this.fs.existsSync(dir)) this.fs.mkdirSync(dir, { recursive: true });

      // Create thumbails folder
      if (!this.fs.existsSync(path.join(dir, FilesService.THUMBNAILS_FOLDER_NAME))) {
         this.fs.mkdirSync(path.join(dir, FilesService.THUMBNAILS_FOLDER_NAME), { recursive: true });
      }

      return dir;
   }

   private async updateStats(userId: number, absolute_path: string, user_agent: string) {
      // Never insert undefined/empty — the DB column may not have a default, and a
      // failed stat write must not be possible here.
      user_agent = user_agent || "unknown";
      const root = await this.getUserRootPath(userId);
      const sanitizedRelative = path.relative(root, absolute_path); // Need this to avoid weird slashes
      const user = await this.userService.getById(userId);

      // Check if file is indexed
      let indexed = await this.uploadedFileRepo.findOne({
         where: { absolute_path: sanitizedRelative, user: { id: userId } },
      });

      // If not index then created it
      if (!indexed) {
         indexed = new UploadedFile();
         indexed.absolute_path = sanitizedRelative;
         indexed.user = user;
         indexed.mime = FilesService.detectFile(absolute_path);
         await this.uploadedFileRepo.save(indexed);
      }

      // Now see if the stat already exists
      let stat = await this.fileAccessStatRepo.findOne({
         where: { user: { id: userId }, uploaded_file: { id: indexed.id }, user_agent },
      });
      if (!stat) {
         stat = new FileAccessStat();
         stat.uploaded_file = indexed;
         stat.user = user;
         stat.count = 0;
         stat.user_agent = user_agent;
         await this.fileAccessStatRepo.save(stat);
      }

      stat.count += 1;
      await this.fileAccessStatRepo.save(stat);
   }

   public async isOwner(userId: number, absolute_path: string) {
      const root = await this.getUserRootPath(userId);
      const sanitizedRelative = path.relative(absolute_path, root);
      // If we replace all "..\" and there is still and email in the path,
      // then the user is trying to access a file outside of his root
      const res = sanitizedRelative.replace(/\.\./g, "").replace(/\\/g, "").replace(/\//g, "");

      const cond = res.length == 0;
      if (!cond) {
         this.logger.log(`Not owner of ${absolute_path}. Sanatized result: ${res}. Sanatized length: ${res.length}`);
      }
      return cond;
   }

   public async invalidateAllThumbnails(): Promise<void> { 
      const users = await this.userRepo.find({ select: { id: true }});
      for (const user of users) {
         const thumbnailFolder = path.join(
            await this.createMetaFolderIfNotExists(user.id),
            FilesService.THUMBNAILS_FOLDER_NAME,
         );
         this.logger.debug(`[::${this.invalidateAllThumbnails.name}] Deleting ${thumbnailFolder}`);
         this.fs.rmdirSync(thumbnailFolder);
      }

      this.logger.debug(`[::${this.invalidateAllThumbnails.name}] Deleting cache keys for all thumbnails`);

      const cachedFileKeys = await this.getCacheKeysFromPattern(`${ThumbnailCacheInterceptor.CachedFilesRedisNamespace}*`);
      if (cachedFileKeys.length > 0) {
         await this.cache.del(...cachedFileKeys);
         this.logger.debug(`[::${this.invalidateAllThumbnails.name}] Deleted keys: ${cachedFileKeys.join(", ")}`);
      } else {
         this.logger.debug(`[::${this.invalidateAllThumbnails.name}] No thumbnail keys found in redis cache`);
      }
   }

   private async invalidateThumbnailsFor(userId: number, uploadedFile: UploadedFile): Promise<void> {
      const thumbnailFolder = path.join(
         await this.createMetaFolderIfNotExists(userId),
         FilesService.THUMBNAILS_FOLDER_NAME,
      );
      const files = this.fs.readdirSync(thumbnailFolder);
      for (const fileEntry of files) {
         if (fileEntry.name.startsWith(`${uploadedFile.id}_`)) {
            this.fs.unlinkSync(path.join(thumbnailFolder, fileEntry.name));
            this.logger.debug(`[::${this.invalidateThumbnailsFor.name}] Deleted thumbnail file ${path.join(thumbnailFolder, fileEntry.name)}`);
         }
      }

      // Invalidate cache
      const cachedFileKeys = await this.getCacheKeysForFile(userId, uploadedFile);
      if (cachedFileKeys.length > 0) {
         await this.cache.del(...cachedFileKeys);
         this.logger.debug(`[::${this.invalidateThumbnailsFor.name}] Deleted keys: ${cachedFileKeys.join(", ")}`);
      } else {
         this.logger.debug(`[::${this.invalidateThumbnailsFor.name}] No keys found in redis cache for file ${uploadedFile.absolute_path}`);
      }
   }

   private async getCacheKeysForFile(userId: number, uploadedFile: UploadedFile): Promise<string[]> {
      const cacheKey = ThumbnailCacheInterceptor.getCacheKey(userId, uploadedFile.absolute_path, 0, 0, false);
      const pattern = `${cacheKey}*`;
      return this.getCacheKeysFromPattern(pattern);
   }

   private async getCacheKeysFromPattern(pattern: string): Promise<string[]> {
      let cursor = "0";
      const keys: string[] = [];

      do {
         // Scan for keys in the Redis store
         const result = await this.cache.scan(cursor, "MATCH", pattern);
         cursor = result[0]; // Update the cursor to the new position
         keys.push(...result[1]); // Push the found keys to the keys array
      } while (cursor !== "0"); // If cursor is '0', the scan is complete

      return keys;      
   }
}
