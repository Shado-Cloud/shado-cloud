import { Readable } from "stream";
import { FilesConstoller } from "src/files/files.controller";
import type { FilesService } from "src/files/files.service";
import type { AppLogger } from "src/logging";

/**
 * The RESPONSE a media request gets from GET /file/:path — status line and headers.
 *
 * The parser is covered in byte-range.test.ts; this is about what actually reaches the client,
 * because the headers ARE the contract a media player reads: `Accept-Ranges` tells it that it may
 * seek, `Content-Range` tells it where in the file it has landed, and `Content-Length` promises how
 * many bytes are coming. A player that is lied to in any of those three either cannot seek or
 * stalls waiting for bytes that never arrive.
 */

const TOTAL = 1000;

/** Captures what the handler wrote, the way a client would see it. */
function fakeRes() {
   const res: any = {
      statusCode: 0,
      headers: {} as Record<string, any>,
      ended: false,
      piped: false,
      writableEnded: false,
      writeHead(status: number, headers?: Record<string, any>) {
         res.statusCode = status;
         Object.assign(res.headers, headers ?? {});
         return res;
      },
      setHeader(k: string, v: any) { res.headers[k] = v; },
      end() { res.ended = true; res.writableEnded = true; },
      on() { return res; },
      once() { return res; },
      emit() { return false; },
      status(code: number) { res.statusCode = code; return res; },
      send() { return res; },
      // `pipeline` needs a real writable to pipe into; a stream sink is enough to observe that
      // bytes were handed over at all.
      write() { res.piped = true; return true; },
      destroy() {},
   };
   return res;
}

function makeController(asStream: jest.Mock) {
   const service = {
      streamInfo: jest.fn().mockResolvedValue({ mime: "audio/ogg", size: TOTAL, is_video: false, is_audio: true }),
      asStream,
   } as unknown as FilesService;
   const logger = { logException: jest.fn(), error: jest.fn(), log: jest.fn() } as unknown as AppLogger;
   return new FilesConstoller(service, logger);
}

async function get(range?: string) {
   const asStream = jest.fn().mockImplementation(async () => Readable.from([Buffer.alloc(8)]));
   const controller = makeController(asStream);
   const res = fakeRes();
   const req: any = { headers: { ...(range ? { range } : {}), "user-agent": "AppleCoreMedia/1.0" } };
   await controller.getFile("Music/song.opus", 1, res, req);
   return { res, asStream };
}

describe("GET /file/:path for audio", () => {
   it("tells a client it may seek, even when sending the whole file", async () => {
      // Without this header AVFoundation treats the resource as a linear stream it cannot address:
      // no precise length, and seeking becomes extrapolation.
      const { res } = await get();
      expect(res.statusCode).toBe(200);
      expect(res.headers["Accept-Ranges"]).toBe("bytes");
      expect(res.headers["Content-Length"]).toBe(TOTAL);
   });

   it("answers a suffix range with the end of the file", async () => {
      // The last bytes are where a container keeps its length (the final Ogg page's granule
      // position), so this is the request that decides whether a client knows how long a song is.
      const { res, asStream } = await get("bytes=-100");
      expect(res.statusCode).toBe(206);
      expect(res.headers["Content-Range"]).toBe(`bytes 900-999/${TOTAL}`);
      expect(res.headers["Content-Length"]).toBe(100);
      expect(asStream).toHaveBeenCalledWith(1, "Music/song.opus", "AppleCoreMedia/1.0", { start: 900, end: 999 });
   });

   it("promises only bytes it can deliver when asked for more than the file holds", async () => {
      const { res } = await get("bytes=0-999999999");
      expect(res.statusCode).toBe(206);
      expect(res.headers["Content-Range"]).toBe(`bytes 0-999/${TOTAL}`);
      expect(res.headers["Content-Length"]).toBe(TOTAL);
   });

   it("serves an open-ended range to the last byte", async () => {
      const { res } = await get("bytes=250-");
      expect(res.statusCode).toBe(206);
      expect(res.headers["Content-Range"]).toBe(`bytes 250-999/${TOTAL}`);
      expect(res.headers["Content-Length"]).toBe(750);
   });

   it("refuses a range outside the file, and says how big it is", async () => {
      const { res, asStream } = await get("bytes=5000-6000");
      expect(res.statusCode).toBe(416);
      expect(res.headers["Content-Range"]).toBe(`bytes */${TOTAL}`);
      expect(res.headers["Accept-Ranges"]).toBe("bytes");
      expect(res.ended).toBe(true);
      expect(asStream).not.toHaveBeenCalled(); // nothing read from disk for a request we reject
   });
});
