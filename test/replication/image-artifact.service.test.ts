import { createHash } from "crypto";
import { PassThrough, Writable } from "stream";
import * as childProcess from "child_process";
import { EventEmitter } from "events";
import { ImageArtifactService } from "src/replication/image-artifact.service";

jest.mock("child_process");

/**
 * `export()` is a multi-gigabyte stream, and its original implementation could hang forever:
 * it registered the write stream's `finish` listener inside the child's `close` handler, after
 * `pipe()` had already ended the stream — so if the flush completed first, the event had already
 * fired and the promise never settled. The deployment step then sat there with no further output
 * and no error, which is unrecoverable without restarting the process.
 *
 * These tests pin the settling behaviour: every path must either resolve or reject.
 */
describe("ImageArtifactService.export", () => {
   let written: Buffer[];
   let files: Record<string, Buffer>;
   let service: ImageArtifactService;

   function fakeFs() {
      return {
         createWriteStream: () =>
            new Writable({
               write(chunk: Buffer, _enc, cb) {
                  written.push(Buffer.from(chunk));
                  // Defer, so the flush genuinely races the child's exit as it does on real IO.
                  setImmediate(cb);
               },
            }),
         existsSync: (p: string) => p in files,
         renameSync: (from: string, to: string) => {
            files[to] = Buffer.concat(written);
            delete files[from];
         },
         unlinkSync: (p: string) => delete files[p],
         readdirSync: () => [],
         statSync: () => ({ mtimeMs: Date.now() }),
      } as never;
   }

   /** A fake `docker save` that emits `payload` then exits with `code`. */
   function mockDockerSave(payload: Buffer | null, code: number, opts: { delayExit?: number } = {}) {
      (childProcess.spawn as jest.Mock).mockImplementation(() => {
         const proc = new EventEmitter() as any;
         proc.stdout = new PassThrough();
         proc.stderr = new PassThrough();
         proc.exitCode = null;
         proc.kill = jest.fn();

         process.nextTick(() => {
            if (payload) proc.stdout.end(payload);
            else proc.stdout.end();
            if (code !== 0) proc.stderr.end(Buffer.from("Error response from daemon: no such image"));
            else proc.stderr.end();
            setTimeout(() => {
               proc.exitCode = code;
               proc.emit("close", code);
            }, opts.delayExit ?? 5);
         });
         return proc;
      });
   }

   beforeEach(() => {
      written = [];
      files = {};
      service = new ImageArtifactService({ get: () => "test-salt" } as never, fakeFs());
   });

   it("resolves with the tarball's real sha256 and size", async () => {
      const payload = Buffer.from("a-tar-archive-of-layers".repeat(500));
      mockDockerSave(payload, 0);

      const artifact = await service.export("sha256:abc", () => undefined);

      expect(artifact.size).toBe(payload.length);
      expect(artifact.tarSha256).toBe(createHash("sha256").update(payload).digest("hex"));
      // The id is the content hash, so an unchanged image re-exports to the same artifact.
      expect(artifact.artifact).toBe(artifact.tarSha256.slice(0, 32));
      expect(Buffer.concat(written)).toEqual(payload);
   });

   it("settles even when the child exits well after the stream has flushed", async () => {
      // The original hang: the write side finished first, so a `finish` listener registered later
      // never fired.
      mockDockerSave(Buffer.from("payload"), 0, { delayExit: 150 });

      await expect(service.export("sha256:abc", () => undefined)).resolves.toMatchObject({ size: 7 });
   });

   it("rejects when docker save fails, rather than trusting a truncated tar", async () => {
      // stdout still closes on failure, so the stream completes normally — only the exit status
      // reveals that the bytes are incomplete.
      mockDockerSave(Buffer.from("partial"), 1);

      await expect(service.export("sha256:missing", () => undefined)).rejects.toThrow(/exited with code 1/);
   });

   it("surfaces docker's stderr in the failure", async () => {
      mockDockerSave(Buffer.from("partial"), 1);

      await expect(service.export("sha256:missing", () => undefined)).rejects.toThrow(/no such image/);
   });

   it("rejects on empty output instead of staging a zero-byte artifact", async () => {
      mockDockerSave(null, 0);

      await expect(service.export("sha256:abc", () => undefined)).rejects.toThrow(/produced no output/);
   });

   it("reports progress while exporting", async () => {
      // Gigabytes take minutes; without progress the step is indistinguishable from hung.
      jest.useFakeTimers({ doNotFake: ["nextTick", "setImmediate", "setTimeout"] });
      const lines: string[] = [];
      mockDockerSave(Buffer.from("x".repeat(1024)), 0);

      const promise = service.export("sha256:abc", (l) => lines.push(l));
      jest.advanceTimersByTime(6000);
      await promise;
      jest.useRealTimers();

      expect(lines.join("")).toMatch(/Exporting sha256:abc/);
      expect(lines.join("")).toMatch(/Exported /);
   });
});
