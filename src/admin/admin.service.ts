import { Inject, Injectable, HttpException, HttpStatus, MessageEvent, Logger } from "@nestjs/common";
import { InjectDataSource, InjectEntityManager } from "@nestjs/typeorm";
import { exec } from "child_process";
import { DataSource, EntityManager } from "typeorm";
import { ConfigService } from "@nestjs/config";
import { EnvVariables } from "../config/config.validator";
import { promisify } from "util";
import { FeatureFlagService } from "./feature-flag.service";
import { FeatureFlagNamespace } from "src/models/admin/featureFlag";
import { DatabaseGetTableRequest } from "./adminApiTypes";
import { EncryptedPassword } from "src/models/EncryptedPassword";
import * as path from "path";
import archiver from "archiver";
import { Observable, Subject } from "rxjs";
import { AbstractFileSystem } from "src/file-system/abstract-file-system.interface";
import { Readable } from "stream";
import { FilesService } from "src/files/files.service";
import { CONFIG_FILE_NAME } from "src/config/config.loader";

@Injectable()
export class AdminService {


   private readonly logger = new Logger(AdminService.name);

   public constructor(
      @Inject() private readonly config: ConfigService<EnvVariables>,
      private readonly featureFlagService: FeatureFlagService,
      @InjectDataSource() private readonly dataSource: DataSource,
      @InjectEntityManager() private readonly entityManager: EntityManager,
      @Inject() private readonly fs: AbstractFileSystem,
      @Inject() private readonly fileService: FilesService,
   ) { }

   /**
    * Database admin methods
    */
   public async getTables() {
      await this.checkDbAccess();

      const tables = this.dataSource.entityMetadatas.map((meta) => meta.tableName);
      return tables.map((table) => {
         const meta = this.entityManager.getRepository(table).metadata;
         return {
            table,
            columns: meta.columns.map((col) => col.propertyName),
            primaryKey: meta.primaryColumns[0]?.propertyName || null,
         };
      });
   }

   public async getTable(tableName: string, request: DatabaseGetTableRequest) {
      const { limit, order_by, order_column, search, search_column } = request;
      await this.checkDbAccess();
      this.validateTableName(tableName);

      const tables = await this.getTables();

      // Verify limit
      if ((!Number.isNaN(parseInt(limit as any)) && limit < 1) || limit > 500) {
         throw new Error("Limit must be between 1 and 500");
      }

      // Verify order_by
      if (!DatabaseGetTableRequest.OrderyByOptions.includes(order_by)) {
         throw new Error("order_by must be either " + DatabaseGetTableRequest.OrderyByOptions.join(" or "));
      }

      // Verify order_column
      const columns = tables.find((entry) => entry.table == tableName)?.columns;
      if (order_column) {
         this.validateColumnName(order_column, columns);
      }

      const qb = this.dataSource
         .createQueryBuilder()
         .select()
         .from(tableName, "t");

      if (search && search_column) {
         this.validateColumnName(search_column, columns);
         qb.where("t." + this.quoteIdentifier(search_column) + " LIKE :search", { search: `%${search}%` });
      }

      if (order_column) {
         qb.orderBy("t." + this.quoteIdentifier(order_column), order_by);
      }

      qb.limit(limit);

      const result = await qb.getRawMany();

      return result;
   }

   private validateTableName(tableName: string, allowCount = false) {
      const tables = this.dataSource.entityMetadatas.map((meta) => meta.tableName);
      if (!tables.includes(tableName)) {
         throw new HttpException(`Table ${tableName} does not exist`, HttpStatus.BAD_REQUEST);
      }
      if (!allowCount && tableName === this.entityManager.getRepository(EncryptedPassword).metadata.tableName) {
         throw new HttpException("Table is encrypted", HttpStatus.FORBIDDEN);
      }
   }

   /**
    * Validates that a column name is a safe identifier (alphanumeric + underscores only)
    * and exists in the provided whitelist. This prevents SQL injection via column names.
    */
   private validateColumnName(column: string, allowedColumns: string[]): string {
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(column)) {
         throw new HttpException("Invalid column name", HttpStatus.BAD_REQUEST);
      }
      if (!allowedColumns.includes(column)) {
         throw new HttpException(`Column ${column} does not exist`, HttpStatus.BAD_REQUEST);
      }
      return column;
   }

   public async getTableCount(tableName: string) {
      await this.checkDbAccess();
      this.validateTableName(tableName, true);

      const result = await this.dataSource
         .createQueryBuilder()
         .select("COUNT(*)", "count")
         .from(tableName, "")
         .getRawOne();

      return { count: parseInt(result?.count || "0") };
   }

   public async deleteRow(tableName: string, id: string) {
      await this.checkDbAccess();
      this.validateTableName(tableName);

      const meta = this.entityManager.getRepository(tableName).metadata;
      const pk = meta.primaryColumns[0]?.propertyName;
      if (!pk) throw new HttpException("No primary key found", HttpStatus.BAD_REQUEST);
      this.validateColumnName(pk, meta.columns.map((c) => c.propertyName));

      await this.dataSource.createQueryBuilder().delete().from(tableName).where(this.quoteIdentifier(pk) + " = :id", { id }).execute();
      return { success: true };
   }

   public async updateRow(tableName: string, id: string, data: Record<string, any>) {
      await this.checkDbAccess();
      this.validateTableName(tableName);

      const meta = this.entityManager.getRepository(tableName).metadata;
      const pk = meta.primaryColumns[0]?.propertyName;
      if (!pk) throw new HttpException("No primary key found", HttpStatus.BAD_REQUEST);

      const validColumns = meta.columns.map((c) => c.propertyName);
      this.validateColumnName(pk, validColumns);

      // Only allow updating columns that pass validation
      const filtered: Record<string, any> = {};
      for (const key of Object.keys(data)) {
         if (key === pk) continue;
         try {
            this.validateColumnName(key, validColumns);
            filtered[key] = data[key];
         } catch {
            // Skip invalid column names silently
         }
      }

      if (Object.keys(filtered).length === 0) {
         throw new HttpException("No valid columns to update", HttpStatus.BAD_REQUEST);
      }

      if (Object.keys(filtered).length === 0) {
         throw new HttpException("No valid columns to update", HttpStatus.BAD_REQUEST);
      }

      await this.dataSource.createQueryBuilder().update(tableName).set(filtered).where(this.quoteIdentifier(pk) + " = :id", { id }).execute();
      return { success: true };
   }

   /**
    * Wraps a column name in backticks to prevent any possibility of SQL injection.
    * Must only be called AFTER validateColumnName().
    */
   private quoteIdentifier(name: string): string {
      return "`" + name.replace(/`/g, "``") + "`";
   }

   private async checkDbAccess() {
      if (await this.featureFlagService.isFeatureFlagDisabled(FeatureFlagNamespace.Admin, "database_api_access")) {
         throw new HttpException("Database API access is disabled", HttpStatus.FORBIDDEN);
      }
   }

   private async execSync(command: string) {
      return promisify(exec)(command);
   }

   public async generateServerSetupBackup(sudoPassword?: string): Promise<Buffer> {
      const tmpDir = `/tmp/server-setup-${Date.now()}`;
      this.fs.mkdirSync(tmpDir, { recursive: true });

      const errors: string[] = [];
      const sudoPrefix = sudoPassword ? `echo '${sudoPassword}' | sudo -S` : "";

      // 1. MySQL dump
      try {
         const dbHost = this.config.get("db.host", { infer: true }) || "localhost";
         const dbUser = this.config.get("db.username", { infer: true });
         const dbPass = this.config.get("db.password", { infer: true });
         const dumpFile = `${tmpDir}/mysql-dump.sql`;
         const baseArgs = `-h ${dbHost} -u ${dbUser} -p'${dbPass}' --protocol=tcp`;
         // Dump everything EXCEPT the data of the huge shado_metrics.metric_datapoint
         // table (6M+ rows). Its structure is still dumped (--no-data, second pass) so a
         // restore recreates the empty table — the metrics service migrations run
         // `ALTER TABLE metric_datapoint ...` on startup and would fail otherwise.
         const dumpCmd =
            `mysqldump ${baseArgs} --all-databases --ignore-table=shado_metrics.metric_datapoint > ${dumpFile} && ` +
            `echo 'USE \`shado_metrics\`;' >> ${dumpFile} && ` +
            `mysqldump ${baseArgs} --no-data shado_metrics metric_datapoint >> ${dumpFile}`;
         await this.execSync(dumpCmd);
      } catch (e) {
         errors.push(`MySQL dump failed: ${(e as Error).message}`);
      }

      // 2-5. Collect config files
      const configErrors = await this.collectServerConfigFiles(tmpDir, sudoPrefix);
      errors.push(...configErrors);

      if (errors.length > 0) {
         this.fs.writeFileSync(`${tmpDir}/errors.txt`, errors.join("\n"));
      }

      const zipBuffer = await this.createZipBuffer(tmpDir);
      this.fs.rmdirSync(tmpDir, { recursive: true });
      return zipBuffer;
   }

   /**
    * Collects Apache configs, .env, Cloudflare tunnel config/credentials, and origin certs into tmpDir.
    * Shared by both the direct and streaming backup methods.
    */
   private async collectServerConfigFiles(tmpDir: string, sudoPrefix: string = ""): Promise<string[]> {
      const errors: string[] = [];
      const isMac = process.platform === "darwin";

      // Apache configs (all sites-available)
      const apacheDir = isMac ? "/opt/homebrew/etc/httpd" : "/etc/apache2/sites-available";
      try {
         this.fs.mkdirSync(`${tmpDir}/apache`, { recursive: true });
         const listCmd = sudoPrefix && !isMac
            ? `${sudoPrefix} ls ${apacheDir}/*.conf 2>/dev/null`
            : `ls ${apacheDir}/*.conf 2>/dev/null`;
         const result = await this.execSync(listCmd);
         const confFiles = result.stdout.trim().split("\n").filter(Boolean);
         for (const file of confFiles) {
            const filename = path.basename(file);
            if (sudoPrefix && !isMac) {
               await this.execSync(`${sudoPrefix} cat ${file} > ${tmpDir}/apache/${filename}`);
            } else {
               const content = this.fs.readFileSync(file, "utf-8");
               this.fs.writeFileSync(`${tmpDir}/apache/${filename}`, content);
            }
         }
      } catch (err) {
         const e = err as Error & { code: string };
         if (e.code === "EACCES" || e.message?.includes("Permission denied")) {
            throw new HttpException("SUDO_REQUIRED", HttpStatus.FORBIDDEN);
         }
         errors.push(`Apache config failed: ${e.message}`);
      }

      // .env file
      try {
         const content = this.fs.readFileSync(path.join(process.cwd(), ".env"), "utf-8");
         this.fs.writeFileSync(`${tmpDir}/env-file.txt`, content);
      } catch (e) {
         errors.push(`.env file failed: ${(e as Error).message}`);
      }

      // config.yml file (for this service)
      try {
         const content = this.fs.readFileSync(path.join(process.cwd(), CONFIG_FILE_NAME), "utf-8");
         this.fs.writeFileSync(`${tmpDir}/config.yml.txt`, content);
      } catch (e) {
         errors.push(`config.yml file failed: ${(e as Error).message}`);
      }

      // Cloudflare tunnel config and credentials
      const cloudflaredDir = "/etc/cloudflared";
      try {
         const configPath = `${cloudflaredDir}/config.yml`;
         if (this.fs.existsSync(configPath)) {
            const content = this.fs.readFileSync(configPath, "utf-8");
            this.fs.writeFileSync(`${tmpDir}/cloudflared-config.yml`, content);
         } else if (sudoPrefix) {
            await this.execSync(`${sudoPrefix} cat ${configPath} > ${tmpDir}/cloudflared-config.yml`);
         }
      } catch (e) {
         errors.push(`Cloudflare config failed: ${(e as Error).message}`);
      }

      try {
         const listCmd = sudoPrefix
            ? `${sudoPrefix} ls ${cloudflaredDir}/*.json 2>/dev/null`
            : `ls ${cloudflaredDir}/*.json 2>/dev/null`;
         const result = await this.execSync(listCmd);
         const jsonFiles = result.stdout.trim().split("\n").filter(Boolean);
         for (const file of jsonFiles) {
            const filename = path.basename(file);
            if (sudoPrefix) {
               await this.execSync(`${sudoPrefix} cat ${file} > ${tmpDir}/${filename}`);
            } else {
               const content = this.fs.readFileSync(file, "utf-8");
               this.fs.writeFileSync(`${tmpDir}/${filename}`, content);
            }
         }
      } catch (e) {
         errors.push(`Cloudflare credentials failed: ${(e as Error).message}`);
      }

      // Cloudflare origin certificate
      for (const certFile of ["/etc/ssl/cloudflare-origin.pem", "/etc/ssl/cloudflare-origin.key"]) {
         try {
            if (this.fs.existsSync(certFile)) {
               const content = this.fs.readFileSync(certFile, "utf-8");
               this.fs.writeFileSync(`${tmpDir}/${path.basename(certFile)}`, content);
            } else if (sudoPrefix) {
               await this.execSync(`${sudoPrefix} cat ${certFile} > ${tmpDir}/${path.basename(certFile)} 2>/dev/null || true`);
            }
         } catch (e) {
            errors.push(`Cloudflare origin cert failed: ${(e as Error).message}`);
         }
      }

      return errors;
   }

   private createZipBuffer(sourceDir: string): Promise<Buffer> {
      return new Promise((resolve, reject) => {
         const chunks: Buffer[] = [];
         const archive = archiver("zip", { zlib: { level: 9 } });

         archive.on("data", (chunk) => chunks.push(chunk));
         archive.on("end", () => resolve(Buffer.concat(chunks)));
         archive.on("error", reject);

         archive.directory(sourceDir, false);
         void archive.finalize();
      });
   }

   public generateServerSetupBackupStream(): Observable<MessageEvent> {
      const subject = new Subject<MessageEvent>();

      this.runServerSetupBackup(subject).catch((e) => {
         subject.next({ data: { type: "error", message: e.message } });
         subject.complete();
      });

      return subject.asObservable();
   }

   private async runServerSetupBackup(subject: Subject<MessageEvent>) {
      const tmpDir = `/tmp/server-setup-${Date.now()}`;
      const zipPath = `/tmp/server-backup-${Date.now()}.zip`;
      this.fs.mkdirSync(tmpDir, { recursive: true });

      // Step 1: MySQL dump with progress
      subject.next({ data: { type: "progress", step: "Dumping MySQL databases...", percent: 0, phase: "mysql" } });
      try {
         const dbHost = this.config.get("db.host", { infer: true }) || "localhost";
         const dbUser = this.config.get("db.username", { infer: true });
         const dbPass = this.config.get("db.password", { infer: true });
         const dumpPath = `${tmpDir}/mysql-dump.sql`;
         const output = this.fs.createWriteStream(dumpPath);
         const { spawn } = require("child_process");
         const commonArgs = ["-h", dbHost, "-u", dbUser, `-p${dbPass}`, "--protocol=tcp"];
         let bytesWritten = 0;

         // Stream one mysqldump invocation into the shared (still-open) output file.
         const runDump = (args: string[]) =>
            new Promise<void>((resolve, reject) => {
               const child = spawn("mysqldump", [...commonArgs, ...args]);

               child.stdout.on("data", (chunk: Buffer) => {
                  bytesWritten += chunk.length;
                  output.write(chunk);
                  subject.next({
                     data: {
                        type: "progress",
                        step: "Dumping MySQL databases...",
                        processedBytes: bytesWritten,
                        phase: "mysql"
                     }
                  });
               });

               child.stderr.on("data", () => { }); // Ignore warnings
               child.on("close", (code: number) => {
                  if (code === 0) resolve();
                  else reject(new Error(`mysqldump exited with code ${code}`));
               });
               child.on("error", reject);
            });

         // Full dump, but skip the DATA of the huge shado_metrics.metric_datapoint table
         // (6M+ rows); the second pass dumps its structure only so a restore recreates the
         // empty table (the metrics migrations run `ALTER TABLE metric_datapoint ...`).
         await runDump(["--all-databases", "--ignore-table=shado_metrics.metric_datapoint"]);
         await new Promise<void>((resolve, reject) =>
            output.write("USE `shado_metrics`;\n", (err) => (err ? reject(err) : resolve())),
         );
         await runDump(["--no-data", "shado_metrics", "metric_datapoint"]);
         output.end();
      } catch (e) {
         this.fs.writeFileSync(`${tmpDir}/mysql-error.txt`, (e as Error).message);
         subject.next({ data: { type: "progress", step: "MySQL dump failed", phase: "mysql" } });
      }

      // Step 2-5: Collect config files (shared with direct backup)
      subject.next({ data: { type: "progress", step: "Copying config files...", phase: "copy" } });
      const errors = await this.collectServerConfigFiles(tmpDir);
      if (errors.length > 0) {
         this.fs.writeFileSync(`${tmpDir}/errors.txt`, errors.join("\n"));
      }

      // Step 6: Create zip with byte progress
      subject.next({ data: { type: "progress", step: "Scanning files...", percent: 0, phase: "zip" } });
      const totalSize = await this.getDirSize(tmpDir);

      const output = this.fs.createWriteStream(zipPath);
      await new Promise<void>((resolve, reject) => {
         const archive = archiver("zip", { zlib: { level: 9 } });

         archive.on("error", reject);
         output.on("close", resolve);

         archive.on("progress", (progress) => {
            const percent = totalSize > 0 ? Math.min(99, Math.round((progress.fs.processedBytes / totalSize) * 100)) : 0;
            subject.next({
               data: {
                  type: "progress",
                  step: "Compressing...",
                  percent,
                  processedBytes: progress.fs.processedBytes,
                  totalBytes: totalSize,
                  phase: "zip"
               }
            });
         });

         archive.pipe(output);
         archive.directory(tmpDir, false);
         void archive.finalize();
      });

      this.fs.rmdirSync(tmpDir, { recursive: true });

      subject.next({ data: { type: "complete", downloadPath: `/admin/backup/download?file=${encodeURIComponent(zipPath)}` } });
      subject.complete();
   }

   public generateCloudBackupStream(): Observable<MessageEvent> {
      const subject = new Subject<MessageEvent>();

      this.runCloudBackup(subject).catch((e) => {
         subject.next({ data: { type: "error", message: e.message } });
         subject.complete();
      });

      return subject.asObservable();
   }

   /**
    * Returns the path of the created backup file
    */
   public async runCloudBackup(subject: Subject<MessageEvent> | null): Promise<string> {
      const cloudDir = this.config.get("this-service.cloud-dir", { infer: true });
      const zipPath = `/tmp/cloud-backup-${Date.now()}.zip`;

      subject?.next({ data: { type: "progress", step: "Scanning files", percent: 0 } });

      // Get total size for progress calculation
      const totalSize = await this.getDirSize(cloudDir);
      let processedSize = 0;

      const output = this.fs.createWriteStream(zipPath);
      await new Promise<void>((resolve, reject) => {
         const archive = archiver("zip", { zlib: { level: 1 } }); // Fast compression for large files

         archive.on("error", reject);
         output.on("close", resolve);

         archive.on("progress", (progress) => {
            processedSize = progress.fs.processedBytes;
            const percent = Math.min(99, Math.round((processedSize / totalSize) * 100));
            subject?.next({ data: { type: "progress", step: "Compressing", percent, processedBytes: processedSize, totalBytes: totalSize } });
         });

         archive.pipe(output);
         archive.directory(cloudDir, "cloud");
         void archive.finalize();
      });

      subject?.next({ data: { type: "complete", downloadPath: `/admin/backup/download?file=${encodeURIComponent(zipPath)}` } });
      subject?.complete();

      return zipPath;
   }

   private async getDirSize(dir: string): Promise<number> {
      let size = 0;
      const files = this.fs.readdirSync(dir);
      for (const file of files) {
         const filePath = path.join(dir, file.name);
         if (file.isDirectory()) {
            size += await this.getDirSize(filePath);
         } else {
            // statSync (not lstat) so a cold-tiered file — a symlink to the cold drive — is measured
            // by its real bytes rather than by the length of the link target.
            //
            // Skip what will not resolve instead of aborting: an unmounted cold drive makes every
            // file on it unresolvable at once, and a disk-usage total is not worth failing the whole
            // admin dashboard over.
            try {
               size += this.fs.statSync(filePath).size;
            } catch (e) {
               this.logger.warn(`Excluding ${filePath} from the size total: ${(e as Error).message}`);
            }
         }
      }
      return size;
   }

   public async getBackupFile(filePath: string): Promise<Readable> {
      if (!filePath.startsWith("/tmp/") || !filePath.includes("-backup-")) {
         throw new HttpException("Invalid file path", HttpStatus.BAD_REQUEST);
      }
      if (!this.fs.existsSync(filePath)) {
         throw new HttpException("Backup file not found or already downloaded", HttpStatus.NOT_FOUND);
      }
      return this.fs.createReadStream(filePath);
   }

   public async deleteBackupFile(filePath: string): Promise<void> {
      try {
         this.fs.unlinkSync(filePath);
      } catch (e) {
         this.logger.error(`Failed to delete backup file ${filePath}: ${(<Error>e).message}`);
      }
   }

   /**
    * Background images management
    */
   private async getBackgroundsDir(): Promise<string> {
      const dir = path.join(this.config.get("this-service.cloud-dir", { infer: true }), "_system", "backgrounds");
      if (!this.fs.existsSync(dir)) {
         this.fs.mkdirSync(dir, { recursive: true });
      }
      return dir;
   }

   public async getBackgroundImages(): Promise<{ images: string[] }> {
      const dir = await this.getBackgroundsDir();
      try {
         const files = this.fs.readdirSync(dir);
         const images = files.map(f => f.name).filter(f => /\.(jpg|jpeg|png|webp|gif)$/i.test(f));
         return { images };
      } catch {
         return { images: [] };
      }
   }

   public async uploadBackgroundImage(file: Express.Multer.File): Promise<{ filename: string }> {
      if (!file) {
         throw new HttpException("No file provided", HttpStatus.BAD_REQUEST);
      }
      if (!/^image\/(jpeg|png|webp|gif)$/.test(file.mimetype)) {
         throw new HttpException("Invalid file type. Only images allowed.", HttpStatus.BAD_REQUEST);
      }

      const dir = await this.getBackgroundsDir();
      const ext = path.extname(file.originalname) || ".jpg";
      const filename = `bg_${Date.now()}${ext}`;
      const filePath = path.join(dir, filename);

      this.fs.writeFileSync(filePath, file.buffer);
      return { filename };
   }

   public async deleteBackgroundImage(filename: string): Promise<void> {
      if (filename.includes("/") || filename.includes("..")) {
         throw new HttpException("Invalid filename", HttpStatus.BAD_REQUEST);
      }
      const filePath = path.join(await this.getBackgroundsDir(), filename);
      if (!this.fs.existsSync(filePath)) {
         throw new HttpException("Image not found", HttpStatus.NOT_FOUND);
      }
      this.fs.unlinkSync(filePath);
   }

   public async getBackgroundImageStream(filename: string): Promise<Readable> {
      if (filename.includes("/") || filename.includes("..")) {
         throw new HttpException("Invalid filename", HttpStatus.BAD_REQUEST);
      }
      const filePath = path.join(await this.getBackgroundsDir(), filename);
      if (!this.fs.existsSync(filePath)) {
         throw new HttpException("Image not found", HttpStatus.NOT_FOUND);
      }
      return this.fs.createReadStream(filePath);
   }

   public invalidateThumbnails() {
      return this.fileService.invalidateAllThumbnails();
   }
}
