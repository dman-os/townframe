import * as zlib from "node:zlib";

// X5 helpers: a minimal PNG decoder (8-bit RGB / RGBA, non-interlaced: what Chromium's screencast
// emits) and the blank-band classifier. No dependency: the pixels are compared with `memcmp`.

export type Image = { width: number; height: number; bpp: number; rows: Buffer };

export function decodePng(png: Buffer): Image {
  if (png.readUInt32BE(0) !== 0x89504e47) throw new Error("not a PNG");
  let off = 8;
  let width = 0, height = 0, bpp = 0;
  const idat: Buffer[] = [];
  while (off < png.length) {
    const len = png.readUInt32BE(off);
    const type = png.toString("ascii", off + 4, off + 8);
    const body = png.subarray(off + 8, off + 8 + len);
    if (type === "IHDR") {
      width = body.readUInt32BE(0);
      height = body.readUInt32BE(4);
      const depth = body[8], color = body[9], interlace = body[12];
      if (depth !== 8 || interlace !== 0 || (color !== 2 && color !== 6)) {
        throw new Error(`unsupported PNG depth ${depth} color ${color} interlace ${interlace}`);
      }
      bpp = color === 6 ? 4 : 3;
    } else if (type === "IDAT") idat.push(body);
    off += 12 + len;
  }
  const raw = zlib.inflateSync(Buffer.concat(idat));
  const stride = width * bpp;
  const rows = Buffer.alloc(stride * height);
  for (let y = 0; y < height; y++) {
    const filter = raw[y * (stride + 1)];
    const src = y * (stride + 1) + 1;
    const dst = y * stride;
    for (let x = 0; x < stride; x++) {
      const a = x >= bpp ? rows[dst + x - bpp] : 0;
      const b = y > 0 ? rows[dst - stride + x] : 0;
      const c = x >= bpp && y > 0 ? rows[dst - stride + x - bpp] : 0;
      let v = raw[src + x];
      switch (filter) {
        case 0: break;
        case 1: v += a; break;
        case 2: v += b; break;
        case 3: v += (a + b) >> 1; break;
        case 4: {
          const p = a + b - c;
          const pa = Math.abs(p - a), pb = Math.abs(p - b), pc = Math.abs(p - c);
          v += pa <= pb && pa <= pc ? a : pb <= pc ? b : c;
          break;
        }
        default: throw new Error(`bad PNG filter ${filter}`);
      }
      rows[dst + x] = v & 255;
    }
  }
  return { width, height, bpp, rows };
}

export type Rect = { x0: number; x1: number; y0: number; y1: number };

/** The most common pixel (as bytes) inside `rect`: the page's background in the rest frame. */
export function backgroundOf(img: Image, rect: Rect): Buffer {
  const counts = new Map<string, number>();
  for (let y = rect.y0; y < rect.y1; y += 3) {
    for (let x = rect.x0; x < rect.x1; x += 3) {
      const o = (y * img.width + x) * img.bpp;
      const k = img.rows.toString("hex", o, o + img.bpp);
      counts.set(k, (counts.get(k) ?? 0) + 1);
    }
  }
  const [best] = [...counts.entries()].sort((a, b) => b[1] - a[1])[0];
  return Buffer.from(best, "hex");
}

/**
 * For each pixel row of `rect`, is every pixel of the row the background colour? Returns the
 * longest vertical run of such rows (px) and the total count. A row with any glyph, gutter or
 * decoration pixel is content.
 */
export function blankRuns(img: Image, rect: Rect, bg: Buffer): { maxRun: number; blankRows: number } {
  const w = rect.x1 - rect.x0;
  const ref = Buffer.alloc(w * img.bpp);
  for (let x = 0; x < w; x++) bg.copy(ref, x * img.bpp);
  let run = 0, maxRun = 0, blankRows = 0;
  for (let y = rect.y0; y < rect.y1; y++) {
    const o = (y * img.width + rect.x0) * img.bpp;
    if (img.rows.compare(ref, 0, ref.length, o, o + ref.length) === 0) {
      blankRows++;
      if (++run > maxRun) maxRun = run;
    } else run = 0;
  }
  return { maxRun, blankRows };
}
