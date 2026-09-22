import {
   Body,
   Controller,
   Delete,
   Get,
   Inject,
   Logger,
   Param,
   Patch,
   Post,
   Query,
   Req,
   Res,
   StreamableFile,
   UploadedFile,
   UseGuards,
   UseInterceptors,
} from "@nestjs/common";
import { JwtAuthGuard } from "src/auth/auth.guard";
import { FileInterceptor } from "@nestjs/platform-express";
import { Throttle } from "@nestjs/throttler";
import { ApiConsumes, ApiOperation, ApiParam, ApiProduces, ApiProperty, ApiResponse, ApiTags } from "@nestjs/swagger";
import { Request, Response } from "express";
import { pipeline } from "stream";
import { AppLogger } from "./../logging";
import { ApiFile, AuthUser } from "src/util";
import { FilesService } from "./files.service";
import {
   FileInfoResponse,
   NewFileRequest,
   OperationStatus,
   OperationStatusResponse,
   OpResWithData,
   RenameFileRequest,
   SaveFileRequest,
   FileBackupsResponse,
} from "./filesApiTypes";
import { ThumbnailCacheInterceptor } from "./thumbnail-cache.interceptor";

@Controller("file")
@UseGuards(JwtAuthGuard)
@ApiTags("Files")
export class FilesConstoller {
   constructor(private readonly fileService: FilesService, @Inject() private readonly logger: AppLogger) {}

   @Get("profile-picture-info")
   @ApiResponse({ description: "Returns profile picture metadata for the authenticated user" })
   public async profilePictureInfo(@AuthUser() userId: number) {
      return this.fileService.profilePictureInfo(userId);
   }

   @Post("upload")
   @ApiOperation({ summary: "Upload a file", description: "Upload a single file. For files larger than 100MB behind a Cloudflare Tunnel, use the chunked upload endpoints instead as Cloudflare's free plan rejects request bodies exceeding 100MB." })
   @ApiResponse({ type: OperationStatusResponse })
   @ApiConsumes("multipart/form-data")
   @ApiFile()
   @UseInterceptors(FileInterceptor("file"))
   public async upload(
      @AuthUser() userId: number,
      @UploadedFile() file: Express.Multer.File,
      @Body() body: { dest: string },
   ) {
      return await this.logger.errorWrapper(async () => {
         const [ok, message] = await this.fileService.upload(userId, file, body.dest);
         if (!ok) throw new Error(message || "Upload failed");
      });
   }

   @Post("upload/chunked/init")
   @ApiOperation({ summary: "Initialize a chunked upload", description: "Starts a chunked upload session. Use this for large files (>100MB) that would be rejected by Cloudflare Tunnel's request body size limit. Returns an uploadId to use for subsequent chunk uploads." })
   @ApiResponse({ type: OperationStatusResponse })
   public async chunkedUploadInit(
      @AuthUser() userId: number,
      @Body() body: { dest: string; filename: string; totalSize: number },
   ) {
      return await this.logger.errorWrapper(async () => {
         return await this.fileService.chunkedUploadInit(userId, body.dest, body.filename, body.totalSize);
      });
   }

   @Post("upload/chunked/:uploadId")
   @ApiOperation({ summary: "Upload a chunk", description: "Uploads a single chunk of a file. Each chunk must be under 100MB to pass through Cloudflare Tunnel. Chunks are identified by index and reassembled on completion." })
   @ApiParam({ name: "uploadId", description: "Upload session ID returned by the init endpoint" })
   @ApiResponse({ type: OperationStatusResponse })
   @ApiConsumes("multipart/form-data")
   @ApiFile()
   @UseInterceptors(FileInterceptor("file"))
   public async chunkedUploadPart(
      @AuthUser() userId: number,
      @UploadedFile() file: Express.Multer.File,
      @Param("uploadId") uploadId: string,
      @Body() body: { index: string },
   ) {
      return await this.logger.errorWrapper(async () => {
         await this.fileService.chunkedUploadPart(userId, uploadId, parseInt(body.index), file);
      });
   }

   @Post("upload/chunked/:uploadId/complete")
   @ApiOperation({ summary: "Complete a chunked upload", description: "Assembles all uploaded chunks into the final file and saves it. Must be called after all chunks have been uploaded." })
   @ApiParam({ name: "uploadId", description: "Upload session ID returned by the init endpoint" })
   @ApiResponse({ type: OperationStatusResponse })
   public async chunkedUploadComplete(
      @AuthUser() userId: number,
      @Param("uploadId") uploadId: string,
   ) {
      return await this.logger.errorWrapper(async () => {
         await this.fileService.chunkedUploadComplete(userId, uploadId);
      });
   }

   @Post("new")
   @ApiResponse({ type: OperationStatusResponse })
   public async new(@Body() body: NewFileRequest, @AuthUser() userId: number): Promise<OperationStatusResponse> {
      return await this.logger.errorWrapper(async () => {
         await this.fileService.new(userId, body.name);
      });
   }

   @Patch("save")
   @ApiResponse({ type: OperationStatusResponse })
   public async save(@Body() body: SaveFileRequest, @AuthUser() userId: number): Promise<OperationStatusResponse> {
      const [success, message] = await this.fileService.save(userId, body.name, body.content, body.append);
      if (success) {
         return {
            status: OperationStatus[OperationStatus.SUCCESS],
            errors: [],
         };
      } else {
         this.logger.logException(new Error(message));
         return {
            status: OperationStatus[OperationStatus.FAILED],
            errors: [{ field: "", message }],
         };
      }
   }

   @Delete("delete")
   @ApiResponse({ type: OperationStatusResponse })
   public async delete(@Body() body: NewFileRequest, @AuthUser() userId: number) {
      const [success, message] = await this.fileService.delete(userId, body.name);
      if (success) {
         return {
            status: OperationStatus[OperationStatus.SUCCESS],
            errors: [],
         };
      } else {
         this.logger.logException(new Error(message));
         return {
            status: OperationStatus[OperationStatus.FAILED],
            errors: [{ field: "", message }],
         };
      }
   }

   @Patch("rename")
   @ApiResponse({ type: OperationStatusResponse })
   public async rename(@Body() body: RenameFileRequest, @AuthUser() userId: number) {
      return await this.logger.errorWrapper(async () => {
         await this.fileService.rename(userId, body.name, body.newName);
      });
   }

   @Get("info/:path")
   @ApiParam({ name: "path" })
   @ApiResponse({ type: FileInfoResponse })
   public async info(@Param("path") path: string, @AuthUser() userId: number): Promise<FileInfoResponse> {
      try {
         const info = await this.fileService.info(userId, path, true, true);
         return {
            status: OperationStatus[OperationStatus.SUCCESS],
            data: info,
            errors: [],
         };
      } catch (e) {
         this.logger.logException(e);
         return {
            status: OperationStatus[OperationStatus.FAILED],
            data: null,
            errors: [],
         };
      }
   }

   @Get("exists/:path")
   @ApiParam({ name: "path" })
   @ApiResponse({ type: OpResWithData })
   public async exists(@Param("path") path: string, @AuthUser() userId: number) {
      try {
         const info = await this.fileService.exists(userId, path);
         return {
            status: OperationStatus[OperationStatus.SUCCESS],
            data: info,
            errors: [],
         };
      } catch (e) {
         this.logger.logException(e);
         return {
            status: OperationStatus[OperationStatus.FAILED],
            data: null,
            errors: [],
         };
      }
   }

   @Get("backups/:path")
   @ApiOperation({ summary: "List backup copies of a file", description: "Reports where copies of a file exist: the primary cloud-dir copy, any configured local mirror disks, and each replica (presence inferred from last sync time and replication ignore rules)." })
   @ApiParam({ name: "path", description: "File relative path + file name + extension" })
   @ApiResponse({ type: FileBackupsResponse })
   public async backups(@Param("path") path: string, @AuthUser() userId: number): Promise<FileBackupsResponse> {
      try {
         const data = await this.fileService.getBackups(userId, path);
         return {
            status: OperationStatus[OperationStatus.SUCCESS],
            data,
            errors: [],
         };
      } catch (e) {
         this.logger.logException(e);
         return {
            status: OperationStatus[OperationStatus.FAILED],
            data: null,
            errors: [{ field: "path", message: (e as Error).message }],
         };
      }
   }

   @Get("thumbnail/:path")
   @UseInterceptors(ThumbnailCacheInterceptor)
   // Thumbnails are read-only, cache-friendly image assets. A single view (a big play queue,
   // a grid of playlists) legitimately fires dozens-to-hundreds of <img> requests at once, so
   // the 1000/min global ceiling was tripping and returning 429 for artwork. Two request paths
   // share the pain: playlist covers hit here directly (keyed on the user's real IP), while song
   // covers are proxied by shado-music-api, which forwards no `cf-connecting-ip` — so ALL of its
   // proxied thumbnail requests collapse onto the one music-api server IP and share a single
   // bucket. Both need a much higher ceiling than a normal API route; this stays a ceiling
   // (blocks pathological scraping) without throttling ordinary browsing.
   @Throttle({ default: { ttl: 60_000, limit: 6000 } })
   @ApiResponse({
      description: "Returns a thumnail stream of the requested file",
   })
   @ApiParam({
      name: "path",
      description: "File relative path + file name + extension",
      type: String,
   })
   public async thumbnail(
      @Param("path") path: string,
      @AuthUser() userId: number,
      @Query("width") width: number | undefined,
      @Query("height") height: number | undefined,
   ) {
      try {
         const stream = await this.fileService.toThumbnail(path, userId, width, height);
         if (!stream) {
            throw new Error("Unable to generate thumbnail for " + path);
         }

         return new StreamableFile(stream);
      } catch (e) {
         this.logger.logException(e);
         return {
            status: OperationStatus[OperationStatus.FAILED],
            errors: [{ field: "path", message: (e as Error).message }],
         };
      }
   }

   /**
    * The byte range a media request is asking for, clamped to the file.
    *
    * `undefined` means it asked for no range (serve the whole file), `"unsatisfiable"` means it
    * asked for bytes the file does not have (416), and otherwise it is the inclusive range to send.
    *
    * The previous parse — split on "-" and `parseInt` both halves — got two cases wrong:
    *
    *   * A SUFFIX range (`bytes=-2048`) asks for the LAST n bytes, which is how a player reads the
    *     structures a container keeps at the end: the final Ogg page, whose granule position IS the
    *     stream's length, or an MP4 `moov` written after the media. Read as a start offset it came
    *     out NaN, `createReadStream` threw, and a legal request was answered with 400 and a JSON
    *     body where the client expected audio.
    *   * `end` was echoed back unclamped, so `bytes=0-999999999` promised a Content-Length of a
    *     billion for a 4MB file — a transfer that can never complete.
    *
    * Both are real defects, but neither turned out to be the cause of the duration errors reported
    * on iOS: measured against an iOS 26 simulator, AVFoundation fetched these files with `bytes=0-1`
    * followed by `bytes=0-<end>` and never asked for a suffix range at all. It over-reports an Ogg
    * Opus length regardless (172.66s for a 170.02s file) and reads the same audio as m4a exactly, so
    * the fault there is the container we store, not the transport. This handler is fixed on its own
    * merits — answering 400 to a valid Range request is wrong however few clients send one.
    *
    * Only the first range of a multi-range request is honoured. A real multipart/byteranges reply is
    * something no media client asks us for, and serving the first is better than failing.
    */
   private static parseByteRange(
      header: string | undefined,
      total: number,
   ): { start: number; end: number } | "unsatisfiable" | undefined {
      if (!header) return undefined;
      const first = header.trim().split(",")[0].trim();
      const match = /^bytes=(\d*)-(\d*)$/.exec(first);
      // Not a form we can serve (a unit other than bytes, or malformed). Ignoring it and sending
      // the whole file is what RFC 9110 prescribes, and is what every client can handle.
      if (!match) return undefined;

      const [, rawStart, rawEnd] = match;
      if (rawStart === "" && rawEnd === "") return undefined;
      if (total <= 0) return "unsatisfiable";

      if (rawStart === "") {
         const lastN = parseInt(rawEnd, 10);
         if (!lastN) return "unsatisfiable"; // `bytes=-0` asks for nothing
         return { start: Math.max(0, total - lastN), end: total - 1 };
      }

      const start = parseInt(rawStart, 10);
      if (start >= total) return "unsatisfiable";
      const end = rawEnd === "" ? total - 1 : Math.min(parseInt(rawEnd, 10), total - 1);
      if (end < start) return "unsatisfiable";
      return { start, end };
   }

   @Get(":path")
   @ApiResponse({ description: "Returns a stream of the requested file" })
   @ApiParam({
      name: "path",
      description: "File relative path + file name + extension",
      type: String,
   })
   public async getFile(
      @Param("path") path: string,
      @AuthUser() userId: number,
      @Res() res: Response,
      @Req() req: Request,
   ) {
      try {
         const fileInto = await this.fileService.streamInfo(userId, path);

         // Tear the file stream down if the client disconnects before the transfer
         // finishes. Without this, an aborted download/seek orphans the underlying
         // read stream (open fd + buffered bytes), which leaks memory over time.
         //
         // `stream.pipeline` guarantees teardown propagates in BOTH directions: if the
         // client aborts (res closes/errors) the source read stream is destroyed —
         // releasing its fd + buffered chunks — and if the source errors the response is
         // ended. Plain `.pipe()` does neither, which is what orphaned the streams.
         const pipeWithCleanup = (stream: NodeJS.ReadableStream) => {
            pipeline(stream, res, (err) => {
               // Client aborts (premature close / connection reset) are the case we WANT
               // to tear down, not real failures — don't log them as errors.
               const code = (err as NodeJS.ErrnoException | null)?.code ?? "";
               if (err && !["ERR_STREAM_PREMATURE_CLOSE", "ECONNRESET", "EPIPE"].includes(code)) {
                  this.logger.logException(err);
               }
            });
         };

         // Media is fetched in pieces — seeking IS a byte range — so this is the path that decides
         // whether a player can seek a song at all, and whether it can work out how long one is.
         if (fileInto.is_video || fileInto.is_audio) {
            const total = fileInto.size;
            const wanted = FilesConstoller.parseByteRange(req.headers.range, total);

            if (wanted === "unsatisfiable") {
               // Telling the client the real size lets it retry with a range that exists, rather
               // than being handed bytes it did not ask for and having to guess what went wrong.
               res.writeHead(416, {
                  "Content-Range": `bytes */${total}`,
                  "Accept-Ranges": "bytes",
               });
               res.end();
            } else if (wanted) {
               const { start, end } = wanted;
               const file = await this.fileService.asStream(userId, path, req.headers["user-agent"], {
                  start,
                  end,
               });
               res.writeHead(206, {
                  "Content-Range": `bytes ${start}-${end}/${total}`,
                  "Accept-Ranges": "bytes",
                  "Content-Length": end - start + 1,
                  "Content-Type": fileInto.mime,
               });

               pipeWithCleanup(file);
            } else {
               res.writeHead(200, {
                  // Advertised even though this response IS the whole file: it is how a client
                  // learns that it may seek. Without it AVFoundation treats the resource as a
                  // linear stream it cannot address — so it never reads the length out of the
                  // container and estimates one from the bitrate instead, and "seeking" becomes
                  // an extrapolation that lands in the wrong place. A browser gets away with the
                  // omission by buffering a small file whole; a phone does not.
                  "Accept-Ranges": "bytes",
                  "Content-Length": total,
                  "Content-Type": fileInto.mime,
               });
               pipeWithCleanup(await this.fileService.asStream(userId, path, req.headers["user-agent"]));
            }
         }
         // Otherwise for any other file just do a simple stream
         else {
            const file = await this.fileService.asStream(userId, path, req.headers["user-agent"]);
            res.writeHead(200, {
               "Content-Type": fileInto.mime,
               "Content-Length": fileInto.size,
            });
            pipeWithCleanup(file);
         }
      } catch (e) {
         this.logger.logException(e);
         res.status(400).send({
            status: OperationStatus[OperationStatus.FAILED],
            errors: [{ field: "path", message: (e as Error).message }],
         });
      }
   }
}
