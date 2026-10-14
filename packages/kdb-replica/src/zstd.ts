/**
 * A synchronous zstd decoder (RFC 8878), decode only.
 *
 * Browsers have no zstd, and peer sync zstd-packs its pages, so the replica carries its own. This
 * is a port of kdb-compression's pure-Kotlin decoder (ZstdDecoder.kt, the one Kotlin/JS ships),
 * keeping its scope and its checks: one-shot decoding of a whole buffer, concatenated and
 * skippable frames, every block and literals type, every sequence table mode, and the optional
 * content checksum (verified when present). Dictionaries are refused - no KDB format uses one.
 * Any input it cannot fully account for throws; it never returns bytes it could not check.
 *
 * It is checked differentially against Node's native zstd and against the Go and Kotlin golden
 * frames in test/zstd.test.ts.
 */

const MAGIC = 0xfd2fb528;
const SKIPPABLE_MAGIC_MASK = 0xfffffff0;
const SKIPPABLE_MAGIC = 0x184d2a50;
const BLOCK_SIZE_MAX = 128 * 1024;

const BLOCK_RAW = 0;
const BLOCK_RLE = 1;
const BLOCK_COMPRESSED = 2;

/** Bytes of zero padding after the input, so the bit readers may load whole words at its end. */
const PAD = 8;

// RFC 8878 §3.1.1.3.2 code tables, indexed LL, OF, ML where three go together.
const MAX_SYMBOL = [35, 31, 52] as const;
const MAX_LOG = [9, 8, 9] as const;

const LL_BASE = new Int32Array([
  0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
  16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 128, 256, 512, 1024, 2048, 4096,
  8192, 16384, 32768, 65536,
]);
const LL_BITS = new Uint8Array([
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12,
  13, 14, 15, 16,
]);
const ML_BASE = new Int32Array([
  3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
  19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
  35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 131, 259, 515, 1027, 2051,
  4099, 8195, 16387, 32771, 65539,
]);
const ML_BITS = new Uint8Array([
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11,
  12, 13, 14, 15, 16,
]);

/** Predefined distributions (RFC 8878 §3.1.1.3.2.2.1), accuracy logs 6, 5 and 6. */
const LL_DEFAULT_NORM = new Int16Array([
  4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1,
  2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1,
  -1, -1, -1, -1,
]);
const ML_DEFAULT_NORM = new Int16Array([
  1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1,
  -1, -1, -1, -1, -1,
]);
const OF_DEFAULT_NORM = new Int16Array([
  1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1,
]);

/** Throws for any input the decoder cannot account for. */
function corrupt(message: string): never {
  throw new Error(`kdb replica: zstd: corrupt data: ${message}`);
}

function readU32(b: Uint8Array, p: number): number {
  return (b[p]! | (b[p + 1]! << 8) | (b[p + 2]! << 16) | (b[p + 3]! << 24)) >>> 0;
}

/** Index of the highest set bit; [v] must be positive. */
function highBit(v: number): number {
  return 31 - Math.clz32(v);
}

/**
 * Decodes every frame in [input] (concatenated zstd frames, skippable frames skipped) and returns
 * the bytes they hold. Throws on malformed input, on a frame that needs a dictionary, and when
 * the output would exceed [maxOutput] bytes - checked against a frame's declared content size
 * before decoding it, when the frame declares one.
 */
export function zstdDecompress(input: Uint8Array, maxOutput: number): Uint8Array {
  const src = new Uint8Array(input.length + PAD);
  src.set(input);
  const end = input.length;
  const out = new OutputBuffer(maxOutput, end);
  let p = 0;
  while (p < end) {
    if (end - p < 4) corrupt(`${end - p} trailing bytes after the last frame`);
    const magic = readU32(src, p);
    if (magic === MAGIC) {
      p = new FrameDecoder(src, end, p + 4, out).decode();
    } else if (((magic & SKIPPABLE_MAGIC_MASK) >>> 0) === SKIPPABLE_MAGIC) {
      if (end - p < 8) corrupt("truncated skippable frame header");
      const size = readU32(src, p + 4);
      if (size > end - p - 8) corrupt("skippable frame runs past the input");
      p += 8 + size;
    } else {
      corrupt(`bad frame magic 0x${magic.toString(16)}`);
    }
  }
  return out.size === out.buf.length ? out.buf : out.buf.slice(0, out.size);
}

/** The whole decoded output across frames, bounded by max. */
class OutputBuffer {
  readonly max: number;
  buf: Uint8Array;
  size = 0;

  constructor(max: number, inputSize: number) {
    this.max = max;
    this.buf = new Uint8Array(Math.max(0, Math.min(max, Math.max(256, inputSize * 4))));
  }

  reserve(n: number): void {
    if (n > this.max - this.size) {
      throw new Error(`kdb replica: zstd: decompressed size exceeds max ${this.max}`);
    }
    const need = this.size + n;
    if (need > this.buf.length) {
      let cap = Math.max(this.buf.length, 256);
      while (cap < need) cap = cap > this.max / 2 ? this.max : cap * 2;
      const grown = new Uint8Array(cap);
      grown.set(this.buf.subarray(0, this.size));
      this.buf = grown;
    }
  }
}

/** FSE decoding table: per state, the symbol, the bits to read, and the base of the next state. */
class FseTable {
  readonly log: number;
  readonly symbol: Uint8Array;
  readonly nbBits: Uint8Array;
  readonly base: Int32Array;

  constructor(log: number, symbol: Uint8Array, nbBits: Uint8Array, base: Int32Array) {
    this.log = log;
    this.symbol = symbol;
    this.nbBits = nbBits;
    this.base = base;
  }

  static build(norm: Int16Array, maxSymbol: number, log: number): FseTable {
    const size = 1 << log;
    const cells = spreadSymbols(norm, maxSymbol, log);
    const next = new Int32Array(maxSymbol + 1);
    for (let s = 0; s <= maxSymbol; s++) next[s] = norm[s] === -1 ? 1 : norm[s]!;
    const nbBits = new Uint8Array(size);
    const base = new Int32Array(size);
    for (let u = 0; u < size; u++) {
      const s = cells[u]!;
      const n = next[s]!;
      next[s] = n + 1;
      const nb = log - highBit(n);
      nbBits[u] = nb;
      base[u] = (n << nb) - size;
    }
    return new FseTable(log, cells, nbBits, base);
  }

  static rle(symbol: number): FseTable {
    return new FseTable(0, new Uint8Array([symbol]), new Uint8Array(1), new Int32Array(1));
  }
}

/**
 * The FSE symbol spread (RFC 8878 §4.1.1): "less than 1" symbols take the last cells, the rest
 * are scattered with a fixed step. Returns the cell -> symbol map.
 */
function spreadSymbols(norm: Int16Array, maxSymbol: number, tableLog: number): Uint8Array {
  const size = 1 << tableLog;
  const cells = new Uint8Array(size);
  let high = size - 1;
  for (let s = 0; s <= maxSymbol; s++) if (norm[s] === -1) cells[high--] = s;
  const step = (size >>> 1) + (size >>> 3) + 3;
  const mask = size - 1;
  let pos = 0;
  for (let s = 0; s <= maxSymbol; s++) {
    const n = norm[s]!;
    for (let k = 0; k < n; k++) {
      cells[pos] = s;
      do {
        pos = (pos + step) & mask;
      } while (pos > high);
    }
  }
  if (pos !== 0) corrupt("FSE table distribution does not fill the table");
  return cells;
}

const DEFAULT_TABLES = [
  FseTable.build(LL_DEFAULT_NORM, LL_DEFAULT_NORM.length - 1, 6),
  FseTable.build(OF_DEFAULT_NORM, OF_DEFAULT_NORM.length - 1, 5),
  FseTable.build(ML_DEFAULT_NORM, ML_DEFAULT_NORM.length - 1, 6),
] as const;

/**
 * Reads a backward bitstream (RFC 8878 §4.1): the last byte holds a 1-bit end marker above the
 * final bits, and fields are read from the highest bit downwards. Bits below the start read as
 * zero; overread reports whether any were taken, which every caller treats as corruption once it
 * is done. The source is zero-padded past its end, so whole-word loads stay in bounds.
 */
class BackwardBits {
  readonly src: Uint8Array;
  readonly start: number;
  /** Bits [0, pos) relative to start are still unread. */
  pos: number;

  constructor(src: Uint8Array, start: number, end: number) {
    if (end <= start) corrupt("empty bitstream");
    const last = src[end - 1]!;
    if (last === 0) corrupt("bitstream has no end marker");
    this.src = src;
    this.start = start;
    this.pos = (end - start - 1) * 8 + highBit(last);
  }

  get overread(): boolean {
    return this.pos < 0;
  }

  get finished(): boolean {
    return this.pos === 0;
  }

  read(n: number): number {
    if (n === 0) return 0;
    if (n > 24) {
      const hi = this.read(n - 16);
      return hi * 0x10000 + this.read(16);
    }
    this.pos -= n;
    return this.extract(this.pos, n);
  }

  /** n <= 24 bits starting at bit low; bits below the start are zero. */
  extract(low: number, n: number): number {
    if (low < 0) {
      const avail = n + low;
      return avail <= 0 ? 0 : this.extract(0, avail) << -low;
    }
    const src = this.src;
    const i = this.start + (low >>> 3);
    const v = src[i]! | (src[i + 1]! << 8) | (src[i + 2]! << 16) | (src[i + 3]! << 24);
    return (v >>> (low & 7)) & ((1 << n) - 1);
  }
}

/** A table description read by readNormalizedCounts. */
interface NormalizedCounts {
  norm: Int16Array;
  maxSymbol: number;
  log: number;
  bytesRead: number;
}

/**
 * Reads an FSE table description (RFC 8878 §4.1.1) from [src] at [p]. Returns the normalized
 * counts (index = symbol), with the accuracy log and the bytes consumed.
 */
function readNormalizedCounts(src: Uint8Array, p: number, end: number, maxSymbol: number, maxLog: number): NormalizedCounts {
  const totalBits = (end - p) * 8;
  let bit = 0;
  const bits = (n: number): number => {
    let v = 0;
    for (let k = 0; k < n; k++) {
      const b = bit + k;
      if (b < totalBits && ((src[p + (b >>> 3)]! >>> (b & 7)) & 1) !== 0) v |= 1 << k;
    }
    return v;
  };

  if (totalBits < 4) corrupt("truncated FSE table description");
  const log = bits(4) + 5;
  bit = 4;
  if (log > maxLog) corrupt(`FSE accuracy log ${log} exceeds ${maxLog}`);
  const norm = new Int16Array(maxSymbol + 1);
  let remaining = (1 << log) + 1;
  let threshold = 1 << log;
  let nbBits = log + 1;
  let symbol = 0;
  let previousZero = false;
  while (remaining > 1) {
    if (previousZero) {
      let n0 = symbol;
      for (;;) {
        const r = bits(2);
        bit += 2;
        if (bit > totalBits) corrupt("truncated FSE table description");
        n0 += r;
        if (r !== 3) break;
      }
      if (n0 > maxSymbol) corrupt("FSE zero run past the maximum symbol");
      symbol = n0;
    }
    if (symbol > maxSymbol) corrupt("FSE table describes too many symbols");
    const max = 2 * threshold - 1 - remaining;
    let count: number;
    const low = bits(nbBits - 1);
    if (low < max) {
      count = low;
      bit += nbBits - 1;
    } else {
      count = bits(nbBits);
      if (count >= threshold) count -= max;
      bit += nbBits;
    }
    if (bit > totalBits) corrupt("truncated FSE table description");
    count--;
    remaining -= count < 0 ? -count : count;
    norm[symbol++] = count;
    previousZero = count === 0;
    if (remaining < threshold) {
      if (remaining <= 1) break;
      nbBits = highBit(remaining) + 1;
      threshold = 1 << (nbBits - 1);
    }
  }
  if (remaining !== 1) corrupt("FSE table probabilities do not add up");
  return { norm, maxSymbol: symbol - 1, log, bytesRead: (bit + 7) >>> 3 };
}

/** A Huffman decoding table of 2^log entries: the symbol and the bits it takes. */
class HuffmanTable {
  readonly log: number;
  readonly symbol: Uint8Array;
  readonly nbBits: Uint8Array;

  constructor(log: number, symbol: Uint8Array, nbBits: Uint8Array) {
    this.log = log;
    this.symbol = symbol;
    this.nbBits = nbBits;
  }
}

class FrameDecoder {
  readonly src: Uint8Array;
  /** End of the real input; src is zero-padded beyond it. */
  readonly end: number;
  p: number;
  readonly out: OutputBuffer;
  readonly frameStart: number;
  readonly reps = new Int32Array([1, 4, 8]);
  huffman: HuffmanTable | undefined;
  /** The current LL, OF and ML tables, kept for the repeat mode. */
  readonly tables: (FseTable | undefined)[] = [undefined, undefined, undefined];
  /** Scratch for Huffman and RLE literals: at most one block's worth. */
  literalScratch: Uint8Array | undefined;
  /** Where the literals section ended, set by decodeLiterals. */
  litEnd = 0;

  constructor(src: Uint8Array, end: number, p: number, out: OutputBuffer) {
    this.src = src;
    this.end = end;
    this.p = p;
    this.out = out;
    this.frameStart = out.size;
  }

  need(n: number): void {
    if (n > this.end - this.p) corrupt("frame truncated");
  }

  /** Decodes one frame whose magic has been consumed; returns the input offset after it. */
  decode(): number {
    const src = this.src;
    const out = this.out;
    this.need(1);
    const fhd = src[this.p++]!;
    const fcsFlag = fhd >>> 6;
    const singleSegment = ((fhd >>> 5) & 1) === 1;
    if (((fhd >>> 3) & 1) !== 0) corrupt("reserved frame header bit is set");
    const checksum = ((fhd >>> 2) & 1) === 1;
    const dictIdSize = [0, 1, 2, 4][fhd & 3]!;
    if (!singleSegment) {
      this.need(1);
      this.p++; // window descriptor: everything is decoded into memory bounded by maxOutput
    }
    this.need(dictIdSize);
    let dictId = 0;
    for (let k = 0; k < dictIdSize; k++) dictId += src[this.p + k]! * 2 ** (8 * k);
    this.p += dictIdSize;
    if (dictId !== 0) {
      throw new Error(`kdb replica: zstd: frame needs dictionary ${dictId}; dictionaries are not supported`);
    }
    const fcsSize = fcsFlag === 0 ? (singleSegment ? 1 : 0) : 1 << fcsFlag;
    this.need(fcsSize);
    let contentSize = -1;
    if (fcsSize > 0) {
      contentSize = 0;
      for (let k = 0; k < fcsSize; k++) contentSize += src[this.p + k]! * 2 ** (8 * k);
      if (fcsSize === 2) contentSize += 256;
      if (fcsSize === 8 && src[this.p + 7]! >= 0x80) corrupt("frame content size overflows");
      this.p += fcsSize;
      if (contentSize > out.max - out.size) {
        throw new Error(`kdb replica: zstd: decompressed size ${contentSize} exceeds max ${out.max}`);
      }
      out.reserve(contentSize);
    }

    for (;;) {
      this.need(3);
      const header = src[this.p]! | (src[this.p + 1]! << 8) | (src[this.p + 2]! << 16);
      this.p += 3;
      const last = (header & 1) === 1;
      const type = (header >>> 1) & 3;
      const size = header >>> 3;
      if (size > BLOCK_SIZE_MAX) corrupt(`block of ${size} bytes exceeds the 128KB maximum`);
      if (type === BLOCK_RAW) {
        this.need(size);
        out.reserve(size);
        out.buf.set(src.subarray(this.p, this.p + size), out.size);
        out.size += size;
        this.p += size;
      } else if (type === BLOCK_RLE) {
        this.need(1);
        out.reserve(size);
        out.buf.fill(src[this.p]!, out.size, out.size + size);
        out.size += size;
        this.p += 1;
      } else if (type === BLOCK_COMPRESSED) {
        this.need(size);
        this.decodeCompressedBlock(this.p, this.p + size);
        this.p += size;
      } else {
        corrupt("reserved block type");
      }
      if (last) break;
    }

    const produced = out.size - this.frameStart;
    if (contentSize >= 0 && produced !== contentSize) {
      corrupt(`frame declares ${contentSize} bytes but decodes to ${produced}`);
    }
    if (checksum) {
      this.need(4);
      const want = readU32(src, this.p);
      this.p += 4;
      const got = xxHash64Low32(out.buf, this.frameStart, produced);
      if (got !== want) corrupt("content checksum mismatch");
    }
    return this.p;
  }

  decodeCompressedBlock(start: number, end: number): void {
    const literals = this.decodeLiterals(start, end);
    this.decodeSequences(this.litEnd, end, literals);
  }

  // --- literals ----------------------------------------------------------------------------------

  scratch(n: number): Uint8Array {
    let s = this.literalScratch;
    if (s === undefined) {
      s = new Uint8Array(BLOCK_SIZE_MAX);
      this.literalScratch = s;
    }
    return s.subarray(0, n);
  }

  /** Returns the block's literals and sets litEnd to the offset after the literals section. */
  decodeLiterals(start: number, end: number): Uint8Array {
    const src = this.src;
    if (start >= end) corrupt("compressed block has no literals section");
    const b0 = src[start]!;
    const type = b0 & 3;
    const sizeFormat = (b0 >>> 2) & 3;
    const byte = (k: number): number => {
      if (start + k >= end) corrupt("truncated literals header");
      return src[start + k]!;
    };
    if (type === 0 || type === 1) {
      let size: number;
      let headerSize: number;
      if (sizeFormat === 0 || sizeFormat === 2) {
        size = b0 >>> 3;
        headerSize = 1;
      } else if (sizeFormat === 1) {
        size = (b0 >>> 4) | (byte(1) << 4);
        headerSize = 2;
      } else {
        size = (b0 >>> 4) | (byte(1) << 4) | (byte(2) << 12);
        headerSize = 3;
      }
      if (size > BLOCK_SIZE_MAX) corrupt("literals section larger than a block");
      const body = start + headerSize;
      if (type === 0) {
        if (size > end - body) corrupt("raw literals run past the block");
        this.litEnd = body + size;
        return src.subarray(body, body + size);
      }
      if (body >= end) corrupt("RLE literals missing their byte");
      this.litEnd = body + 1;
      return this.scratch(size).fill(src[body]!);
    }

    const streams4 = sizeFormat !== 0;
    let regen: number;
    let compressed: number;
    let headerSize: number;
    if (sizeFormat === 0 || sizeFormat === 1) {
      regen = (b0 >>> 4) | ((byte(1) & 0x3f) << 4);
      compressed = (byte(1) >>> 6) | (byte(2) << 2);
      headerSize = 3;
    } else if (sizeFormat === 2) {
      regen = (b0 >>> 4) | (byte(1) << 4) | ((byte(2) & 3) << 12);
      compressed = (byte(2) >>> 2) | (byte(3) << 6);
      headerSize = 4;
    } else {
      regen = (b0 >>> 4) | (byte(1) << 4) | ((byte(2) & 0x3f) << 12);
      compressed = (byte(2) >>> 6) | (byte(3) << 2) | (byte(4) << 10);
      headerSize = 5;
    }
    if (regen > BLOCK_SIZE_MAX) corrupt("literals section larger than a block");
    const body = start + headerSize;
    if (compressed > end - body) corrupt("compressed literals run past the block");
    let streamStart = body;
    if (type === 2) {
      const consumed = this.readHuffmanTable(body, body + compressed);
      streamStart += consumed;
    } else if (this.huffman === undefined) {
      corrupt("treeless literals with no previous Huffman table");
    }
    const literals = this.scratch(regen);
    this.decodeHuffmanStreams(this.huffman!, streamStart, body + compressed, literals, streams4);
    this.litEnd = body + compressed;
    return literals;
  }

  /** Reads a Huffman tree description into this.huffman; returns the bytes it took. */
  readHuffmanTable(start: number, end: number): number {
    const src = this.src;
    if (start >= end) corrupt("missing Huffman tree description");
    const header = src[start]!;
    const weights = new Uint8Array(256);
    let count: number;
    let consumed: number;
    if (header >= 128) {
      count = header - 127;
      consumed = 1 + ((count + 1) >>> 1);
      if (consumed > end - start) corrupt("truncated Huffman weights");
      for (let i = 0; i < count; i++) {
        const b = src[start + 1 + (i >>> 1)]!;
        weights[i] = (i & 1) === 0 ? b >>> 4 : b & 0xf;
      }
    } else {
      consumed = 1 + header;
      if (header === 0 || consumed > end - start) corrupt("truncated FSE-compressed Huffman weights");
      const wEnd = start + consumed;
      const counts = readNormalizedCounts(src, start + 1, wEnd, 12, 6);
      const table = FseTable.build(counts.norm, counts.maxSymbol, counts.log);
      const bits = new BackwardBits(src, start + 1 + counts.bytesRead, wEnd);
      let s1 = bits.read(table.log);
      let s2 = bits.read(table.log);
      count = 0;
      // Two interleaved states; when the stream runs out, the other state's symbol is the last.
      for (;;) {
        if (count > 253) corrupt("too many Huffman weights");
        weights[count++] = table.symbol[s1]!;
        s1 = table.base[s1]! + bits.read(table.nbBits[s1]!);
        if (bits.overread) {
          weights[count++] = table.symbol[s2]!;
          break;
        }
        weights[count++] = table.symbol[s2]!;
        s2 = table.base[s2]! + bits.read(table.nbBits[s2]!);
        if (bits.overread) {
          weights[count++] = table.symbol[s1]!;
          break;
        }
      }
    }
    if (count > 255) corrupt("too many Huffman weights");
    let total = 0;
    for (let i = 0; i < count; i++) {
      const w = weights[i]!;
      if (w > 11) corrupt(`Huffman weight ${w} too large`);
      if (w > 0) total += 1 << (w - 1);
    }
    if (total === 0) corrupt("Huffman weights are all zero");
    const log = highBit(total) + 1;
    if (log > 11) corrupt("Huffman table deeper than 11 bits");
    const rest = (1 << log) - total;
    if ((rest & (rest - 1)) !== 0) corrupt("Huffman weights do not complete a prefix code");
    weights[count++] = highBit(rest) + 1;
    const rank = new Int32Array(13);
    for (let i = 0; i < count; i++) rank[weights[i]!]!++;
    if (rank[1]! < 2 || (rank[1]! & 1) !== 0) corrupt("Huffman weights do not complete a prefix code");

    const size = 1 << log;
    const symbol = new Uint8Array(size);
    const nbBits = new Uint8Array(size);
    const next = new Int32Array(13);
    let pos = 0;
    for (let w = 1; w <= log; w++) {
      next[w] = pos;
      pos += rank[w]! << (w - 1);
    }
    for (let s = 0; s < count; s++) {
      const w = weights[s]!;
      if (w === 0) continue;
      const len = 1 << (w - 1);
      const from = next[w]!;
      symbol.fill(s, from, from + len);
      nbBits.fill(log + 1 - w, from, from + len);
      next[w] = from + len;
    }
    this.huffman = new HuffmanTable(log, symbol, nbBits);
    return consumed;
  }

  decodeHuffmanStreams(table: HuffmanTable, start: number, end: number, out: Uint8Array, streams4: boolean): void {
    if (!streams4) {
      this.decodeHuffmanStream(table, start, end, out, 0, out.length);
      return;
    }
    const src = this.src;
    if (end - start < 10) corrupt("4-stream literals too short for a jump table");
    const s1 = src[start]! | (src[start + 1]! << 8);
    const s2 = src[start + 2]! | (src[start + 3]! << 8);
    const s3 = src[start + 4]! | (src[start + 5]! << 8);
    const b1 = start + 6;
    const b2 = b1 + s1;
    const b3 = b2 + s2;
    const b4 = b3 + s3;
    if (b4 >= end) corrupt("Huffman jump table points past the literals");
    const seg = (out.length + 3) >>> 2;
    const last = out.length - 3 * seg;
    if (last < 0) corrupt("4-stream literals too short to split");
    this.decodeHuffmanStream(table, b1, b2, out, 0, seg);
    this.decodeHuffmanStream(table, b2, b3, out, seg, seg);
    this.decodeHuffmanStream(table, b3, b4, out, 2 * seg, seg);
    this.decodeHuffmanStream(table, b4, end, out, 3 * seg, last);
  }

  /** BackwardBits peek/skip, inlined: this loop is where most of the decode time goes. */
  decodeHuffmanStream(table: HuffmanTable, start: number, end: number, out: Uint8Array, at: number, n: number): void {
    const bits = new BackwardBits(this.src, start, end);
    const src = this.src;
    const log = table.log;
    const mask = (1 << log) - 1;
    const symbols = table.symbol;
    const lengths = table.nbBits;
    let pos = bits.pos;
    for (let i = at, stop = at + n; i < stop; i++) {
      const low = pos - log;
      let e: number;
      if (low >= 0) {
        const j = start + (low >>> 3);
        e = ((src[j]! | (src[j + 1]! << 8) | (src[j + 2]! << 16)) >>> (low & 7)) & mask;
      } else {
        e = bits.extract(low, log);
      }
      out[i] = symbols[e]!;
      pos -= lengths[e]!;
    }
    if (pos !== 0) corrupt("Huffman stream not consumed exactly");
  }

  // --- sequences ---------------------------------------------------------------------------------

  decodeSequences(start: number, end: number, literals: Uint8Array): void {
    const src = this.src;
    const out = this.out;
    if (start >= end) corrupt("compressed block has no sequences section");
    let q = start;
    const b0 = src[q++]!;
    let nbSeq: number;
    if (b0 < 128) {
      nbSeq = b0;
    } else if (b0 < 255) {
      if (q >= end) corrupt("truncated sequence count");
      nbSeq = ((b0 - 128) << 8) + src[q++]!;
    } else {
      if (q + 1 >= end) corrupt("truncated sequence count");
      nbSeq = src[q]! + (src[q + 1]! << 8) + 0x7f00;
      q += 2;
    }
    if (nbSeq === 0) {
      if (q !== end) corrupt("bytes after an empty sequences section");
      out.reserve(literals.length);
      out.buf.set(literals, out.size);
      out.size += literals.length;
      return;
    }
    if (q >= end) corrupt("missing sequence compression modes");
    const modes = src[q++]!;
    if ((modes & 3) !== 0) corrupt("reserved sequence mode bits are set");
    q = this.readTable(q, end, modes >>> 6, 0);
    q = this.readTable(q, end, (modes >>> 4) & 3, 1);
    q = this.readTable(q, end, (modes >>> 2) & 3, 2);
    const ll = this.tables[0]!;
    const of = this.tables[1]!;
    const ml = this.tables[2]!;
    const reps = this.reps;
    const frameStart = this.frameStart;

    const bits = new BackwardBits(src, q, end);
    let llState = bits.read(ll.log);
    let ofState = bits.read(of.log);
    let mlState = bits.read(ml.log);
    let litPos = 0;
    for (let n = 0; n < nbSeq; n++) {
      const ofCode = of.symbol[ofState]!;
      const mlCode = ml.symbol[mlState]!;
      const llCode = ll.symbol[llState]!;
      // Codes above 30 name offsets past 2GB, beyond any output this decoder can produce.
      if (ofCode > 30) corrupt(`offset code ${ofCode}`);
      const ofValue = 2 ** ofCode + bits.read(ofCode);
      const matchLen = ML_BASE[mlCode]! + bits.read(ML_BITS[mlCode]!);
      const litLen = LL_BASE[llCode]! + bits.read(LL_BITS[llCode]!);
      if (bits.overread) corrupt("sequence bitstream overread");

      let offset: number;
      if (ofValue > 3) {
        offset = ofValue - 3;
        reps[2] = reps[1]!;
        reps[1] = reps[0]!;
        reps[0] = offset;
      } else {
        const idx = ofValue - 1 + (litLen === 0 ? 1 : 0);
        if (idx === 0) {
          offset = reps[0]!;
        } else if (idx === 1) {
          offset = reps[1]!;
          reps[1] = reps[0]!;
          reps[0] = offset;
        } else {
          offset = idx === 2 ? reps[2]! : reps[0]! - 1;
          reps[2] = reps[1]!;
          reps[1] = reps[0]!;
          reps[0] = offset;
        }
      }

      if (litLen > literals.length - litPos) corrupt("sequence copies more literals than the block holds");
      out.reserve(litLen);
      let buf = out.buf;
      if (litLen > 16) {
        buf.set(literals.subarray(litPos, litPos + litLen), out.size);
      } else {
        for (let k = 0, to = out.size; k < litLen; k++) buf[to + k] = literals[litPos + k]!;
      }
      out.size += litLen;
      litPos += litLen;

      if (offset <= 0 || offset > out.size - frameStart) corrupt(`match offset ${offset} reaches before the frame`);
      out.reserve(matchLen);
      buf = out.buf;
      let from = out.size - offset;
      let to = out.size;
      if (offset >= matchLen && matchLen > 32) {
        buf.copyWithin(to, from, from + matchLen);
      } else {
        for (const stop = to + matchLen; to < stop; ) buf[to++] = buf[from++]!;
      }
      out.size += matchLen;

      if (n !== nbSeq - 1) {
        llState = ll.base[llState]! + bits.read(ll.nbBits[llState]!);
        mlState = ml.base[mlState]! + bits.read(ml.nbBits[mlState]!);
        ofState = of.base[ofState]! + bits.read(of.nbBits[ofState]!);
      }
    }
    if (!bits.finished) corrupt("sequence bitstream not consumed exactly");
    const rest = literals.length - litPos;
    out.reserve(rest);
    out.buf.set(literals.subarray(litPos), out.size);
    out.size += rest;
  }

  /** Reads (or selects) the table for kind (0 LL, 1 OF, 2 ML) in mode; returns the offset after its description. */
  readTable(q: number, end: number, mode: number, kind: 0 | 1 | 2): number {
    let next = q;
    let table: FseTable | undefined;
    if (mode === 0) {
      table = DEFAULT_TABLES[kind];
    } else if (mode === 1) {
      if (q >= end) corrupt("missing RLE symbol");
      const s = this.src[q]!;
      if (s > MAX_SYMBOL[kind]) corrupt(`RLE symbol ${s} out of range`);
      next = q + 1;
      table = FseTable.rle(s);
    } else if (mode === 2) {
      const counts = readNormalizedCounts(this.src, q, end, MAX_SYMBOL[kind], MAX_LOG[kind]);
      next = q + counts.bytesRead;
      table = FseTable.build(counts.norm, counts.maxSymbol, counts.log);
    } else {
      table = this.tables[kind];
      if (table === undefined) corrupt("repeat sequence table with no previous table");
    }
    this.tables[kind] = table;
    return next;
  }
}

// --- XXH64 ----------------------------------------------------------------------------------------
//
// The content checksum is the low 32 bits of XXH64 with seed 0 (RFC 8878 §3.1.1). 64-bit values
// are carried as (hi, lo) uint32 pairs rather than BigInt, which would be orders of magnitude
// slower; each helper leaves its result in rh/rl.

let rh = 0;
let rl = 0;

const P1H = 0x9e3779b1, P1L = 0x85ebca87;
const P2H = 0xc2b2ae3d, P2L = 0x27d4eb4f;
const P3H = 0x165667b1, P3L = 0x9e3779f9;
const P4H = 0x85ebca77, P4L = 0xc2b2ae63;
const P5H = 0x27d4eb2f, P5L = 0x165667c5;

function add64(ah: number, al: number, bh: number, bl: number): void {
  const lo = al + bl;
  rl = lo >>> 0;
  rh = (ah + bh + (lo > 0xffffffff ? 1 : 0)) >>> 0;
}

/** The low 64 bits of a * b. */
function mul64(ah: number, al: number, bh: number, bl: number): void {
  const a0 = al & 0xffff, a1 = al >>> 16;
  const b0 = bl & 0xffff, b1 = bl >>> 16;
  const p00 = a0 * b0, p01 = a0 * b1, p10 = a1 * b0, p11 = a1 * b1;
  const mid = (p00 >>> 16) + (p01 & 0xffff) + (p10 & 0xffff);
  rl = (((mid & 0xffff) << 16) | (p00 & 0xffff)) >>> 0;
  const hi = p11 + (p01 >>> 16) + (p10 >>> 16) + (mid >>> 16);
  rh = (hi + Math.imul(ah, bl) + Math.imul(al, bh)) >>> 0;
}

/** Rotate left by r, 0 < r < 32. */
function rotl64(h: number, l: number, r: number): void {
  rh = ((h << r) | (l >>> (32 - r))) >>> 0;
  rl = ((l << r) | (h >>> (32 - r))) >>> 0;
}

/** round(acc, input) = rotl(acc + input * P2, 31) * P1. */
function xxRound(ah: number, al: number, ih: number, il: number): void {
  mul64(ih, il, P2H, P2L);
  add64(ah, al, rh, rl);
  rotl64(rh, rl, 31);
  mul64(rh, rl, P1H, P1L);
}

/** mergeRound(acc, v) = (acc ^ round(0, v)) * P1 + P4. */
function xxMerge(ah: number, al: number, vh: number, vl: number): void {
  xxRound(0, 0, vh, vl);
  mul64((ah ^ rh) >>> 0, (al ^ rl) >>> 0, P1H, P1L);
  add64(rh, rl, P4H, P4L);
}

/**
 * xxRound on lane (lanes[at], lanes[at + 1]) with the 8 bytes at data[p], all in locals: the
 * stripe loop is the whole cost of checksumming, and going through rh/rl here halves its speed.
 */
function stripeRound(lanes: Uint32Array, at: number, data: Uint8Array, p: number): void {
  const il = readU32(data, p), ih = readU32(data, p + 4);
  // t = input * P2 (low 64 bits)
  let a0 = il & 0xffff, a1 = il >>> 16;
  let p00 = a0 * (P2L & 0xffff), p01 = a0 * (P2L >>> 16), p10 = a1 * (P2L & 0xffff);
  let mid = (p00 >>> 16) + (p01 & 0xffff) + (p10 & 0xffff);
  let tl = (((mid & 0xffff) << 16) | (p00 & 0xffff)) >>> 0;
  let th = a1 * (P2L >>> 16) + (p01 >>> 16) + (p10 >>> 16) + (mid >>> 16) + Math.imul(ih, P2L) + Math.imul(il, P2H);
  // acc + t
  const sl = lanes[at + 1]! + tl;
  let xl = sl >>> 0;
  let xh = (lanes[at]! + th + (sl > 0xffffffff ? 1 : 0)) >>> 0;
  // rotl 31
  const rl31 = ((xl << 31) | (xh >>> 1)) >>> 0;
  xh = ((xh << 31) | (xl >>> 1)) >>> 0;
  xl = rl31;
  // * P1
  a0 = xl & 0xffff;
  a1 = xl >>> 16;
  p00 = a0 * (P1L & 0xffff);
  p01 = a0 * (P1L >>> 16);
  p10 = a1 * (P1L & 0xffff);
  mid = (p00 >>> 16) + (p01 & 0xffff) + (p10 & 0xffff);
  tl = (((mid & 0xffff) << 16) | (p00 & 0xffff)) >>> 0;
  th = a1 * (P1L >>> 16) + (p01 >>> 16) + (p10 >>> 16) + (mid >>> 16) + Math.imul(xh, P1L) + Math.imul(xl, P1H);
  lanes[at] = th;
  lanes[at + 1] = tl;
}

/** The low 32 bits of XXH64(data[offset, offset + length)), seed 0. */
function xxHash64Low32(data: Uint8Array, offset: number, length: number): number {
  const end = offset + length;
  let p = offset;
  let hh: number, hl: number;
  if (length >= 32) {
    add64(P1H, P1L, P2H, P2L);
    const lanes = new Uint32Array([rh, rl, P2H, P2L, 0, 0, 0, 0]);
    // -P1
    add64(~P1H >>> 0, ~P1L >>> 0, 0, 1);
    lanes[6] = rh;
    lanes[7] = rl;
    const limit = end - 32;
    while (p <= limit) {
      stripeRound(lanes, 0, data, p);
      stripeRound(lanes, 2, data, p + 8);
      stripeRound(lanes, 4, data, p + 16);
      stripeRound(lanes, 6, data, p + 24);
      p += 32;
    }
    const v1h = lanes[0]!, v1l = lanes[1]!, v2h = lanes[2]!, v2l = lanes[3]!;
    const v3h = lanes[4]!, v3l = lanes[5]!, v4h = lanes[6]!, v4l = lanes[7]!;
    rotl64(v1h, v1l, 1);
    hh = rh; hl = rl;
    rotl64(v2h, v2l, 7);
    add64(hh, hl, rh, rl);
    hh = rh; hl = rl;
    rotl64(v3h, v3l, 12);
    add64(hh, hl, rh, rl);
    hh = rh; hl = rl;
    rotl64(v4h, v4l, 18);
    add64(hh, hl, rh, rl);
    xxMerge(rh, rl, v1h, v1l);
    xxMerge(rh, rl, v2h, v2l);
    xxMerge(rh, rl, v3h, v3l);
    xxMerge(rh, rl, v4h, v4l);
    hh = rh; hl = rl;
  } else {
    hh = P5H; hl = P5L;
  }
  add64(hh, hl, Math.floor(length / 0x100000000) >>> 0, length >>> 0);
  hh = rh; hl = rl;
  while (p + 8 <= end) {
    xxRound(0, 0, readU32(data, p + 4), readU32(data, p));
    rotl64((hh ^ rh) >>> 0, (hl ^ rl) >>> 0, 27);
    mul64(rh, rl, P1H, P1L);
    add64(rh, rl, P4H, P4L);
    hh = rh; hl = rl;
    p += 8;
  }
  if (p + 4 <= end) {
    mul64(0, readU32(data, p), P1H, P1L);
    rotl64((hh ^ rh) >>> 0, (hl ^ rl) >>> 0, 23);
    mul64(rh, rl, P2H, P2L);
    add64(rh, rl, P3H, P3L);
    hh = rh; hl = rl;
    p += 4;
  }
  while (p < end) {
    mul64(0, data[p]!, P5H, P5L);
    rotl64((hh ^ rh) >>> 0, (hl ^ rl) >>> 0, 11);
    mul64(rh, rl, P1H, P1L);
    hh = rh; hl = rl;
    p++;
  }
  // h ^= h >>> 33; h *= P2; h ^= h >>> 29; h *= P3; h ^= h >>> 32
  hl = (hl ^ (hh >>> 1)) >>> 0;
  mul64(hh, hl, P2H, P2L);
  hh = rh; hl = rl;
  hl = (hl ^ ((hl >>> 29) | (hh << 3))) >>> 0;
  hh = (hh ^ (hh >>> 29)) >>> 0;
  mul64(hh, hl, P3H, P3L);
  return (rl ^ rh) >>> 0;
}
