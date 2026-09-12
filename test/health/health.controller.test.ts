import { Test, TestingModule } from "@nestjs/testing";
import { ConfigService } from "@nestjs/config";
import { HealthController } from "src/health/health.controller";
import { ReplicationRole } from "src/config/config.validator";

/**
 * The health endpoint is unauthenticated and is what gates the replica updater's rollback
 * decision, so two properties matter: it must answer without touching any dependency, and it
 * must not disclose configuration to an anonymous caller.
 */
describe("HealthController", () => {
   async function build(role: ReplicationRole | undefined) {
      const module: TestingModule = await Test.createTestingModule({
         controllers: [HealthController],
         providers: [
            {
               provide: ConfigService,
               useValue: {
                  get: jest.fn((key: string) => (key === "this-service.replication.role" ? role : undefined)),
               },
            },
         ],
      }).compile();
      return module.get<HealthController>(HealthController);
   }

   afterEach(() => {
      delete process.env.SHADO_IMAGE_ID;
   });

   it("reports ok with the node's replication role", async () => {
      const controller = await build(ReplicationRole.Replica);
      const report = controller.getHealth();

      expect(report.ok).toBe(true);
      expect(report.role).toBe("replica");
   });

   it("falls back to 'unknown' rather than throwing when the role is unset", async () => {
      const controller = await build(undefined);
      expect(controller.getHealth().role).toBe("unknown");
   });

   it("resolves the running build version", async () => {
      const controller = await build(ReplicationRole.Primary);
      // Read from package.json next to cwd, since the runtime image starts node directly and
      // npm_package_version is therefore absent.
      expect(controller.getHealth().version).toMatch(/^\d+\.\d+\.\d+/);
   });

   it("reports uptime so a crash-looping container is distinguishable from a healthy one", async () => {
      const controller = await build(ReplicationRole.Replica);
      const report = controller.getHealth();

      expect(typeof report.uptime).toBe("number");
      expect(report.uptime).toBeGreaterThanOrEqual(0);
   });

   it("surfaces the running image id when the updater provided one", async () => {
      process.env.SHADO_IMAGE_ID = "sha256:abc123";
      const controller = await build(ReplicationRole.Replica);
      expect(controller.getHealth().image).toBe("sha256:abc123");
   });

   it("omits the image id when unset, rather than reporting an empty string", async () => {
      const controller = await build(ReplicationRole.Replica);
      expect(controller.getHealth().image).toBeUndefined();
   });

   it("discloses nothing beyond ok, role, version, uptime and image", async () => {
      const controller = await build(ReplicationRole.Primary);
      // Guards against a future field leaking config to anonymous callers.
      expect(Object.keys(controller.getHealth()).sort()).toEqual(["image", "ok", "role", "uptime", "version"]);
   });
});
