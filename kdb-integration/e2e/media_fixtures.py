"""Deterministic test media for the attachment e2e tests (docs/kdb-attachments-e2e-test-plan.md).

Every generator writes a file and returns a MediaTruth: the file's SHA-256 and size plus what the
media itself says (frame count and a hash per frame, sample count, dimensions). A test checks
byte integrity with the SHA-256 and media integrity by re-deriving the truth from the bytes it
downloaded: a file can only pass the second check if it still parses frame by frame as the same
media.

Raw formats (Y4M video, WAV audio, PNG, random binary) are written here from a seed with the
standard library only, so the bytes are identical on every run and every platform, and an edit
lands at a known offset. MP4/H.264 comes from ffmpeg (a required test dependency) and is checked
by decoding every frame (ffmpeg's framemd5).
"""

from __future__ import annotations

import hashlib
import math
import os
import random
import shutil
import struct
import sys
import subprocess
import zlib
from array import array
from dataclasses import dataclass, field
from pathlib import Path

MiB = 1024 * 1024


@dataclass(frozen=True)
class MediaTruth:
    kind: str                  # "y4m", "wav", "png", "bin", "mp4"
    sha256: str
    size: int
    frames: int = 0            # video frames, or audio samples for wav
    frame_hashes: tuple[str, ...] = field(default_factory=tuple)
    width: int = 0
    height: int = 0
    title: str = ""            # wav INFO/INAM

    def same_media(self, other: "MediaTruth") -> bool:
        return (self.kind, self.frames, self.frame_hashes, self.width, self.height) == \
            (other.kind, other.frames, other.frame_hashes, other.width, other.height)


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(MiB), b""):
            h.update(block)
    return h.hexdigest()


# -- random binary ---------------------------------------------------------------------------

def stream_bin(seed: int, size: int, block: int = 4 * MiB):
    """Yields size seeded random bytes in blocks: the same stream for the same seed, never held
    whole, so a multi-GB file can be generated straight to disk or into an upload."""
    rng = random.Random(seed)
    left = size
    while left > 0:
        n = min(block, left)
        yield rng.randbytes(n)
        left -= n


def make_bin(path: str | Path, size: int, seed: int = 1) -> MediaTruth:
    h = hashlib.sha256()
    with open(path, "wb") as f:
        for b in stream_bin(seed, size):
            h.update(b)
            f.write(b)
    return MediaTruth(kind="bin", sha256=h.hexdigest(), size=size)


def bin_truth(path: str | Path) -> MediaTruth:
    return MediaTruth(kind="bin", sha256=sha256_file(path), size=os.path.getsize(path))


def insert_bytes(src: str | Path, dst: str | Path, offset: int, data: bytes) -> MediaTruth:
    """Copies src to dst with data inserted at offset: every later byte shifts."""
    with open(src, "rb") as f, open(dst, "wb") as out:
        out.write(f.read(offset))
        out.write(data)
        shutil.copyfileobj(f, out, MiB)
    return bin_truth(dst)


# -- Y4M raw video ---------------------------------------------------------------------------

def _y4m_frame(seed: int, index: int, w: int, h: int) -> bytes:
    """One 4:2:0 frame: a gradient that moves with the frame index, plus a seeded noise band so
    every frame (and every seed) is distinct."""
    shift = (index * 7 + seed * 13) % 256
    row = bytes((x + shift) & 255 for x in range(w + h))
    y_plane = b"".join(row[y:y + w] for y in range(h))
    noise = random.Random((seed << 20) ^ index).randbytes(w * 8)
    y_plane = noise + y_plane[len(noise):]
    cw, ch = w // 2, h // 2
    u_plane = bytes([(index * 3 + seed) & 255]) * (cw * ch)
    v_plane = bytes([(255 - index * 5 - seed) & 255]) * (cw * ch)
    return y_plane + u_plane + v_plane


def y4m_frame_bytes(w: int, h: int) -> int:
    return w * h + 2 * (w // 2) * (h // 2)


def make_y4m(path: str | Path, frames: int, w: int = 320, h: int = 240, seed: int = 1,
             frame_seeds: dict[int, int] | None = None) -> MediaTruth:
    """Writes frames frames; frame_seeds overrides the seed of chosen frames (an edit)."""
    frame_seeds = frame_seeds or {}
    with open(path, "wb") as f:
        f.write(f"YUV4MPEG2 W{w} H{h} F25:1 Ip A1:1 C420jpeg\n".encode())
        for i in range(frames):
            f.write(b"FRAME\n")
            f.write(_y4m_frame(frame_seeds.get(i, seed), i, w, h))
    return y4m_truth(path)


def y4m_truth(path: str | Path) -> MediaTruth:
    """Parses a Y4M file frame by frame. Raises ValueError if it is not one."""
    with open(path, "rb") as f:
        header = f.readline()
        if not header.startswith(b"YUV4MPEG2 "):
            raise ValueError("not a Y4M stream")
        params = {p[0]: p[1:] for p in header.decode().split()[1:]}
        w, h = int(params["W"]), int(params["H"])
        size = y4m_frame_bytes(w, h)
        hashes = []
        while True:
            marker = f.readline()
            if not marker:
                break
            if not marker.startswith(b"FRAME"):
                raise ValueError(f"frame {len(hashes)}: bad marker {marker[:16]!r}")
            body = f.read(size)
            if len(body) != size:
                raise ValueError(f"frame {len(hashes)}: truncated ({len(body)} of {size} bytes)")
            hashes.append(hashlib.sha256(body).hexdigest())
    return MediaTruth(kind="y4m", sha256=sha256_file(path), size=os.path.getsize(path),
                      frames=len(hashes), frame_hashes=tuple(hashes), width=w, height=h)


def y4m_header_len(w: int = 320, h: int = 240) -> int:
    return len(f"YUV4MPEG2 W{w} H{h} F25:1 Ip A1:1 C420jpeg\n")


def replace_frames(dst: str | Path, frames: int, replaced: range, w: int = 320, h: int = 240,
                   seed: int = 1, new_seed: int = 99) -> MediaTruth:
    """The same video with the frames in replaced re-rendered under new_seed: bytes change only
    inside those frames."""
    return make_y4m(dst, frames, w, h, seed, {i: new_seed for i in replaced})


# -- WAV audio -------------------------------------------------------------------------------

def make_wav(path: str | Path, seconds: float = 2.0, rate: int = 22050, seed: int = 1,
             title: str = "kdb test tone") -> MediaTruth:
    """16-bit mono PCM: a seeded frequency sweep, with a LIST/INFO INAM title chunk before the
    data, so retitling changes a few header bytes and nothing else."""
    n = int(seconds * rate)
    f0 = 220.0 + (seed % 17) * 20
    samples = array("h", (int(12000 * math.sin(2 * math.pi * (f0 + 200.0 * i / n) * i / rate))
                          for i in range(n)))
    if samples.itemsize != 2:
        raise RuntimeError("array('h') is not 16-bit here")
    if sys.byteorder != "little":
        samples.byteswap()
    data = samples.tobytes()
    name = title.encode() + b"\0"
    if len(name) % 2:
        name += b"\0"
    info = b"INFO" + b"INAM" + struct.pack("<I", len(name)) + name
    fmt = struct.pack("<HHIIHH", 1, 1, rate, rate * 2, 2, 16)
    body = (b"WAVE" + b"fmt " + struct.pack("<I", len(fmt)) + fmt
            + b"LIST" + struct.pack("<I", len(info)) + info
            + b"data" + struct.pack("<I", len(data)) + data)
    with open(path, "wb") as f:
        f.write(b"RIFF" + struct.pack("<I", len(body)) + body)
    return wav_truth(path)


def wav_truth(path: str | Path) -> MediaTruth:
    raw = Path(path).read_bytes()
    if raw[:4] != b"RIFF" or raw[8:12] != b"WAVE":
        raise ValueError("not a WAV file")
    pos, title, data = 12, "", None
    while pos + 8 <= len(raw):
        cid, clen = raw[pos:pos + 4], struct.unpack("<I", raw[pos + 4:pos + 8])[0]
        chunk = raw[pos + 8:pos + 8 + clen]
        if len(chunk) != clen:
            raise ValueError(f"chunk {cid!r} truncated")
        if cid == b"LIST" and chunk[:4] == b"INFO" and chunk[4:8] == b"INAM":
            title = chunk[12:].split(b"\0", 1)[0].decode()
        if cid == b"data":
            data = chunk
        pos += 8 + clen + (clen & 1)
    if data is None:
        raise ValueError("no data chunk")
    # One "frame" per 4096 samples, hashed, so a damaged region is located, not just detected.
    step = 8192
    hashes = tuple(hashlib.sha256(data[i:i + step]).hexdigest() for i in range(0, len(data), step))
    return MediaTruth(kind="wav", sha256=hashlib.sha256(raw).hexdigest(), size=len(raw),
                      frames=len(data) // 2, frame_hashes=hashes, title=title)


# -- PNG -------------------------------------------------------------------------------------

def make_png(path: str | Path, w: int = 64, h: int = 64, seed: int = 1) -> MediaTruth:
    rng = random.Random(seed)
    rows = b"".join(b"\0" + rng.randbytes(w * 3) for _ in range(h))

    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))

    png = (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
           + chunk(b"IDAT", zlib.compress(rows, 9)) + chunk(b"IEND", b""))
    Path(path).write_bytes(png)
    return png_truth(path)


def png_truth(path: str | Path) -> MediaTruth:
    raw = Path(path).read_bytes()
    if raw[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError("not a PNG")
    pos, idat, w, h = 8, b"", 0, 0
    while pos < len(raw):
        n = struct.unpack(">I", raw[pos:pos + 4])[0]
        kind, data = raw[pos + 4:pos + 8], raw[pos + 8:pos + 8 + n]
        if struct.unpack(">I", raw[pos + 8 + n:pos + 12 + n])[0] != zlib.crc32(kind + data):
            raise ValueError(f"PNG chunk {kind!r}: bad CRC")
        if kind == b"IHDR":
            w, h = struct.unpack(">II", data[:8])
        if kind == b"IDAT":
            idat += data
        pos += 12 + n
    pixels = zlib.decompress(idat)
    return MediaTruth(kind="png", sha256=hashlib.sha256(raw).hexdigest(), size=len(raw),
                      frames=1, frame_hashes=(hashlib.sha256(pixels).hexdigest(),), width=w, height=h)


# -- MP4 via ffmpeg --------------------------------------------------------------------------

def ffmpeg() -> str:
    exe = shutil.which("ffmpeg")
    if not exe:
        raise RuntimeError("ffmpeg is required for the attachment media tests (brew install ffmpeg / "
                           "apt-get install ffmpeg)")
    return exe


def make_mp4(path: str | Path, seconds: int = 2, w: int = 320, h: int = 240, crf: int = 23) -> MediaTruth:
    subprocess.run([ffmpeg(), "-v", "error", "-y", "-f", "lavfi", "-i",
                    f"testsrc2=size={w}x{h}:rate=25", "-t", str(seconds), "-c:v", "libx264",
                    "-preset", "ultrafast", "-crf", str(crf), "-pix_fmt", "yuv420p", "-g", "25",
                    "-threads", "1", "-bitexact", "-map_metadata", "-1", str(path)],
                   check=True, capture_output=True, timeout=120)
    return mp4_truth(path)


def reencode_mp4(src: str | Path, dst: str | Path, crf: int = 30) -> MediaTruth:
    subprocess.run([ffmpeg(), "-v", "error", "-y", "-i", str(src), "-c:v", "libx264", "-preset",
                    "ultrafast", "-crf", str(crf), "-threads", "1", "-bitexact", str(dst)],
                   check=True, capture_output=True, timeout=120)
    return mp4_truth(dst)


def mp4_truth(path: str | Path) -> MediaTruth:
    """Decodes every frame: a file whose bytes were damaged or reordered fails here even if it
    still has a plausible container."""
    r = subprocess.run([ffmpeg(), "-v", "error", "-i", str(path), "-f", "framemd5", "-"],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0 or r.stderr.strip():
        raise ValueError(f"ffmpeg could not decode {path}: {r.stderr.strip()[:500]}")
    hashes = tuple(line.rsplit(",", 1)[1].strip() for line in r.stdout.splitlines()
                   if line and not line.startswith("#"))
    return MediaTruth(kind="mp4", sha256=sha256_file(path), size=os.path.getsize(path),
                      frames=len(hashes), frame_hashes=hashes)


# -- dispatch --------------------------------------------------------------------------------

def truth_of(kind: str, path: str | Path) -> MediaTruth:
    return {"y4m": y4m_truth, "wav": wav_truth, "png": png_truth, "bin": bin_truth,
            "mp4": mp4_truth}[kind](path)
