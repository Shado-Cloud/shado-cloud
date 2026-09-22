import { FilesConstoller } from "src/files/files.controller";

/**
 * The Range parser behind GET /file/:path for audio and video.
 *
 * Seeking a song IS a byte range, and a player works out how long a song is by reading the parts of
 * a container that describe it — which for Ogg and for a late-`moov` MP4 live at the END of the
 * file, fetched with a suffix range. So every case below is a playback behaviour, not a protocol
 * nicety: the two that used to be wrong (a suffix range read as a start offset, an over-long end
 * echoed back unclamped) are why an iOS client reported lengths tens of seconds out and glitched
 * when seeking, while a browser — which buffers a small file whole — was unaffected.
 */
const parse = (header: string | undefined, total: number) =>
   (FilesConstoller as unknown as {
      parseByteRange(h: string | undefined, t: number): { start: number; end: number } | "unsatisfiable" | undefined;
   }).parseByteRange(header, total);

describe("byte range parsing", () => {
   const TOTAL = 1000;

   it("serves the whole file when nothing was asked for", () => {
      expect(parse(undefined, TOTAL)).toBeUndefined();
      expect(parse("", TOTAL)).toBeUndefined();
   });

   it("reads an explicit range", () => {
      expect(parse("bytes=0-499", TOTAL)).toEqual({ start: 0, end: 499 });
      expect(parse("bytes=500-999", TOTAL)).toEqual({ start: 500, end: 999 });
   });

   it("runs an open-ended range to the last byte", () => {
      expect(parse("bytes=500-", TOTAL)).toEqual({ start: 500, end: 999 });
      expect(parse("bytes=0-", TOTAL)).toEqual({ start: 0, end: 999 });
   });

   it("clamps an end past the file instead of promising bytes it cannot send", () => {
      // AVFoundation asks for deliberately over-long ranges. Echoing one back set a Content-Length
      // the response could never satisfy, and the transfer stalled mid-track.
      expect(parse("bytes=0-999999999", TOTAL)).toEqual({ start: 0, end: 999 });
      expect(parse("bytes=900-5000", TOTAL)).toEqual({ start: 900, end: 999 });
   });

   it("reads a suffix range as the LAST n bytes", () => {
      // How a player reads the end of a container — the final Ogg page's granule position is the
      // stream's real length. Parsed as a start offset this came out NaN, so the client never got
      // that length and estimated one from the bitrate instead.
      expect(parse("bytes=-2048", TOTAL)).toEqual({ start: 0, end: 999 });
      expect(parse("bytes=-100", TOTAL)).toEqual({ start: 900, end: 999 });
      expect(parse("bytes=-1", TOTAL)).toEqual({ start: 999, end: 999 });
   });

   it("rejects a range the file cannot satisfy", () => {
      expect(parse("bytes=1000-", TOTAL)).toBe("unsatisfiable");
      expect(parse("bytes=1200-1300", TOTAL)).toBe("unsatisfiable");
      expect(parse("bytes=600-500", TOTAL)).toBe("unsatisfiable");
      expect(parse("bytes=-0", TOTAL)).toBe("unsatisfiable");
      expect(parse("bytes=0-100", 0)).toBe("unsatisfiable");
   });

   it("honours the first range of a multi-range request", () => {
      expect(parse("bytes=0-99,200-299", TOTAL)).toEqual({ start: 0, end: 99 });
   });

   it("ignores a unit it cannot serve and sends the whole file", () => {
      expect(parse("items=0-10", TOTAL)).toBeUndefined();
      expect(parse("bytes=abc-def", TOTAL)).toBeUndefined();
   });
});
