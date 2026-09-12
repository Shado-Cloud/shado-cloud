import { Module } from "@nestjs/common";
import { AdminController } from "./admin.controller";
import { AdminService } from "./admin.service";
import { TypeOrmModule } from "@nestjs/typeorm";
import { User } from "./../models/user";
import { FeatureFlagService } from "./feature-flag.service";
import { FeatureFlag } from "src/models/admin/featureFlag";
import { ServiceFunctionsController } from "./service-functions/service-functions.controller";
import { ServiceFunction } from "../models/admin/serviceFunction";
import { FilesModule } from "../files/files.module";
import { EmailService } from "./email.service";
import { DirectoriesModule } from "../directories/directories.module";
import { RemoteTerminalGateway } from "./remote-terminal.gateway";
import { DeploymentController } from "./deployment.controller";
import { DeploymentService } from "./deployment.service";
import { DeploymentProject } from "../models/admin/deploymentProject";
import { TwoFactorGuard } from "./two-factor.guard";
import { CronAdminService } from "./cron.service";
import { ReplicaPropagationService } from "./replica-propagation.service";
import { ImageBuildService } from "./image-build.service";
import { ReplicationModule } from "../replication/replication.module";

@Module({
   controllers: [AdminController, ServiceFunctionsController, DeploymentController],
   imports: [TypeOrmModule.forFeature([User, FeatureFlag, ServiceFunction, DeploymentProject]),
      FilesModule,
      DirectoriesModule,
      // For ImageArtifactService: the deployment pipeline stages built images there, and the
      // replication endpoints stream them to replicas.
      ReplicationModule,
   ],
   providers: [AdminService, FeatureFlagService, EmailService, RemoteTerminalGateway, DeploymentService, ReplicaPropagationService, ImageBuildService, TwoFactorGuard, CronAdminService],
   exports: [AdminService],
})
export class AdminModule { }
