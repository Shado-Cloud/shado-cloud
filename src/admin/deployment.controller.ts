import {
   Body,
   Controller,
   Delete,
   Get,
   HttpCode,
   HttpException,
   HttpStatus,
   Inject,
   Param,
   Patch,
   Post,
   Put,
   Headers,
   UnauthorizedException,
   Sse,
   MessageEvent,
   UseGuards,
} from "@nestjs/common";
import { Observable } from "rxjs";
import { JwtAuthGuard } from "src/auth/auth.guard";
import { ApiParam, ApiTags } from "@nestjs/swagger";
import { Logger } from "@nestjs/common";
import { AdminGuard } from "./admin.strategy";
import crypto from "crypto";
import { ConfigService } from "@nestjs/config";
import { EnvVariables } from "src/config/config.validator";
import { DeploymentService } from "./deployment.service";
import * as path from "path";
import { AbstractFileSystem } from "src/file-system/abstract-file-system.interface";
import { CONFIG_FILE_NAME } from "src/config/config.loader";
import * as fs from "fs";

@Controller("admin")
@ApiTags("Deployment")
export class DeploymentController {
   private readonly logger = new Logger(DeploymentController.name);

   constructor(
      private readonly deploymentService: DeploymentService,
      private readonly config: ConfigService<EnvVariables>,
      @Inject() private readonly abstractFs: AbstractFileSystem,
   ) {}

   @Post("redeploy/:slug")
   @HttpCode(HttpStatus.OK)
   @ApiParam({ name: "slug", description: "Project slug to deploy" })
   async redeploy(
      @Param("slug") slug: string,
      @Body() payload: any,
      @Headers("x-hub-signature-256") signature: string,
   ) {
      const project = await this.deploymentService.getProject(slug);
      if (!project) {
         throw new HttpException("Invalid deployment project", HttpStatus.BAD_REQUEST);
      }

      this.logger.log(`Received webhook payload for ${slug}`);

      try {
         if (payload.ref !== `refs/heads/${project.branch}`) {
            this.logger.warn(`Ignoring push to non-${project.branch} branch`);
            return { message: `Not a push to ${project.branch} branch, ignoring` };
         }

         const headCommitMsg = payload.head_commit?.message || "";
         if (headCommitMsg.includes("[skip deploy]")) {
            this.logger.log("Skipping deployment: commit message contains [skip deploy]");
            return { message: "Skipped: commit contains [skip deploy]" };
         }

         const githubSecret = this.config.get("this-service.deployment.github-webhook-secret", { infer: true });
         if (!githubSecret) {
            throw new Error("env var this-service.deployment.github-webhook-secret is undefined");
         }

         const hash = crypto.createHmac("sha256", githubSecret).update(JSON.stringify(payload)).digest("hex");
         if (`sha256=${hash}` !== signature) {
            this.logger.warn("Invalid signature");
            throw new UnauthorizedException("Invalid signature");
         }

         this.logger.log("Starting redeployment...");
         if (await this.deploymentService.isRunning()) {
            await this.deploymentService.enqueue(slug, "github-webhook");
            return { message: "Deployment queued" };
         }
         void this.deploymentService.startDeployment(slug, "github-webhook");

         return { message: "Deployment triggered successfully" };
      } catch (error) {
         const e = <Error>error;
         this.logger.error("Deployment failed " + e.message, e.stack);
         return { message: e.message };
      }
   }

   @Get("deployment/projects")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public getDeploymentProjects() {
      return this.deploymentService.getProjects();
   }

   @Post("deployment/projects")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public async createDeploymentProject(@Body() body: { slug: string; name: string; workDir: string; pm2ProcessName?: string; branch?: string; steps: any[] }) {
      const { DeploymentProject } = await import("../models/admin/deploymentProject");
      const project = new DeploymentProject();
      project.slug = body.slug;
      project.name = body.name;
      project.workDir = body.workDir;
      project.pm2ProcessName = body.pm2ProcessName || null;
      project.branch = body.branch || "master";
      project.setSteps(body.steps);
      return this.deploymentService.saveProject(project);
   }

   @Put("deployment/projects/:slug")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public async updateDeploymentProject(@Param("slug") slug: string, @Body() body: Partial<{ name: string; workDir: string; pm2ProcessName: string; branch: string; steps: any[]; enabled: boolean }>) {
      const project = await this.deploymentService.getProject(slug);
      if (!project) throw new HttpException("Project not found", HttpStatus.NOT_FOUND);
      if (body.name !== undefined) project.name = body.name;
      if (body.workDir !== undefined) project.workDir = body.workDir;
      if (body.pm2ProcessName !== undefined) project.pm2ProcessName = body.pm2ProcessName;
      if (body.branch !== undefined) project.branch = body.branch;
      if (body.steps !== undefined) project.setSteps(body.steps);
      if (body.enabled !== undefined) project.enabled = body.enabled;
      return this.deploymentService.saveProject(project);
   }

   @Delete("deployment/projects/:slug")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public async deleteDeploymentProject(@Param("slug") slug: string) {
      await this.deploymentService.deleteProject(slug);
      return { success: true };
   }

   @Get("deployment/steps/:project")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public getDeploymentSteps(@Param("project") project: string) {
      return this.deploymentService.getSteps(project);
   }

   @Patch("deployment/steps/:project/:step/skip")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public async toggleStepSkip(@Param("project") projectSlug: string, @Param("step") step: string, @Body("skip") skip: boolean) {
      const project = await this.deploymentService.getProject(projectSlug);
      if (!project) throw new HttpException("Project not found", HttpStatus.NOT_FOUND);
      const steps = project.getSteps();
      const target = steps.find(s => s.step === step);
      if (!target) throw new HttpException("Step not found", HttpStatus.NOT_FOUND);
      target.skip = skip;
      project.setSteps(steps);
      await this.deploymentService.saveProject(project);
      return { success: true };
   }

   @Get("deployment/replicas")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public getConnectedReplicas() {
      return this.deploymentService.connectedReplicas();
   }

   @Get("deployment/status")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public async getDeploymentStatus() {
      return {
         isRunning: await this.deploymentService.isRunning(),
         current: await this.deploymentService.getCurrentDeployment(),
         last: await this.deploymentService.getLastDeployment(),
         queue: await this.deploymentService.getQueue(),
      };
   }

   @Get("deployment/stream")
   @UseGuards(JwtAuthGuard, AdminGuard)
   @Sse()
   public streamDeployment(): Observable<MessageEvent> {
      const subject = this.deploymentService.getSubject();
      if (!subject) {
         throw new HttpException("No deployment in progress", HttpStatus.NOT_FOUND);
      }
      return subject.asObservable();
   }

   @Get("deployment/start/:project")
   @UseGuards(JwtAuthGuard, AdminGuard)
   @Sse()
   public async startDeployment(@Param("project") project: string): Promise<Observable<MessageEvent>> {
      const subject = await this.deploymentService.startDeployment(project, "admin");
      return subject.asObservable();
   }

   @Post("deployment/cancel")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public async cancelDeployment(): Promise<{ success: boolean }> {
      await this.deploymentService.cancelDeployment();
      return { success: true };
   }

   @Get("deployment/retry/:step")
   @UseGuards(JwtAuthGuard, AdminGuard)
   @Sse()
   public async retryStep(@Param("step") step: string): Promise<Observable<MessageEvent>> {
      const subject = await this.deploymentService.retryStep(step);
      return subject.asObservable();
   }

   @Get("env/:project")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public async getEnvFile(@Param("project") projectSlug: string): Promise<string> {
      const project = await this.deploymentService.getProject(projectSlug);
      if (!project) throw new HttpException("Project not found", HttpStatus.NOT_FOUND);

      const workDir = this.deploymentService.resolveWorkDir(project);
      const envPath = path.join(workDir, ".env");

      if (this.abstractFs.existsSync(envPath)) {
         return this.abstractFs.readFileSync(envPath, "utf-8") as string;
      }

      const configYmlPath = path.join(workDir, CONFIG_FILE_NAME);
      if (this.abstractFs.existsSync(configYmlPath)) {
         return this.abstractFs.readFileSync(configYmlPath, "utf-8") as string;
      }

      throw new HttpException("Env file not found", HttpStatus.NOT_FOUND);
   }

   @Put("env/:project")
   @UseGuards(JwtAuthGuard, AdminGuard)
   public async saveEnvFile(
      @Param("project") projectSlug: string,
      @Body("content") content: string,
   ): Promise<{ success: boolean; message?: string }> {
      const project = await this.deploymentService.getProject(projectSlug);
      if (!project) throw new HttpException("Project not found", HttpStatus.NOT_FOUND);

      const workDir = this.deploymentService.resolveWorkDir(project);

      if (!workDir) {
         throw new HttpException("Working directory not configured", HttpStatus.BAD_REQUEST);
      }

      if (this.abstractFs.existsSync(path.join(workDir, ".env")) ||
         this.abstractFs.existsSync(path.join(workDir, ".example.env")) || fs.existsSync(path.join(workDir, ".env.example"))) {
         this.abstractFs.writeFileSync(path.join(workDir, ".env"), content, "utf-8");
         this.logger.log(`Env file updated for ${projectSlug}`);
         return { success: true };
      }

      if (this.abstractFs.existsSync(path.join(workDir, CONFIG_FILE_NAME))) {
         this.abstractFs.writeFileSync(path.join(workDir, CONFIG_FILE_NAME), content, "utf-8");
         this.logger.log(`config.yml file updated for ${projectSlug}`);
         return { success: true };
      }

      this.logger.error(`Project ${projectSlug} is using unkown env structure (not .env and not config.yml)`);
      return { success: false, message: `Project ${projectSlug} is using unkown env structure (not .env and not config.yml)` };
   }
}
