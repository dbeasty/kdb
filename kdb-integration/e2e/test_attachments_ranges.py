"""Verified ranged reads of attachments (docs/kdb-attachments-plan.md; Go client
OpenAttachmentRange). Every byte a ranged read returns has been checked against the attachment's
content hash through its chunk's RFC 6962 inclusion proof, so a range is as trustworthy as a
whole read - these tests read media by range the way a player or a resuming download would.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import threading
import time

import pytest

import media_fixtures as mf
from attachment_fixtures import (
    attach, attachment_dir, doc_id, read_attachment)
from server_fixtures import KdbServer, upsert

pytestmark = [pytest.mark.server]

MiB = 1024 * 1024


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@pytest.fixture
def srv():
    s = KdbServer().start()
    yield s
    s.stop()


def put(srv: KdbServer, n: int, name: str, path) -> str:
    doc = doc_id(n)
    upsert(srv, doc, {"id": doc})
    attach(srv, doc, name, path)
    return doc


def test_video_frames_read_by_byte_range(srv, tmp_path):
    """Frames of a raw video fetched by range - including frames that straddle a 1 MiB chunk
    boundary, and the last frame - are exactly the frames recorded at generation."""
    truth = mf.make_y4m(tmp_path / "v.y4m", 40)
    doc = put(srv, 0xF100, "v.y4m", tmp_path / "v.y4m")
    fb, hdr = mf.y4m_frame_bytes(320, 240), mf.y4m_header_len()
    starts = {k: hdr + k * (6 + fb) + 6 for k in range(40)}
    straddling = [k for k in range(40) if starts[k] // MiB != (starts[k] + fb - 1) // MiB]
    picks = sorted({0, 7, 39, *straddling[:3]})
    assert straddling, "no frame crosses a chunk boundary - the test needs one"
    for k in picks:
        got = read_attachment(srv, doc, "v.y4m", offset=starts[k], length=fb)
        assert got.get("size") == fb, (k, got)
        assert got["sha256"] == truth.frame_hashes[k], f"frame {k} read by range differs"


def test_mp4_keyframes_read_by_byte_range(srv, tmp_path):
    """Seek like a player: ffprobe locates each keyframe packet in the file (byte position and
    size); fetching exactly that range returns that packet's bytes."""
    mf.make_mp4(tmp_path / "m.mp4", seconds=4)
    raw = (tmp_path / "m.mp4").read_bytes()
    doc = put(srv, 0xF200, "m.mp4", tmp_path / "m.mp4")
    r = subprocess.run([mf.ffmpeg().replace("ffmpeg", "ffprobe"), "-v", "error", "-select_streams", "v:0",
                        "-show_entries", "packet=pos,size,flags", "-of", "json", str(tmp_path / "m.mp4")],
                       capture_output=True, text=True, check=True, timeout=60)
    keys = [(int(p["pos"]), int(p["size"])) for p in json.loads(r.stdout)["packets"] if "K" in p["flags"]]
    assert len(keys) >= 3, keys
    for pos, size in keys:
        got = read_attachment(srv, doc, "m.mp4", offset=pos, length=size)
        assert (got.get("size"), got.get("sha256")) == (size, sha(raw[pos:pos + size])), (pos, size, got)


def test_arbitrary_ranges_match_the_file(srv, tmp_path):
    """Ranges at the start, inside one chunk, across several chunks, at the last byte, running
    past the end (stops at the end), at the end (empty), and past it (typed range error)."""
    size = 5 * MiB + 321
    mf.make_bin(tmp_path / "b", size, seed=9)
    raw = (tmp_path / "b").read_bytes()
    doc = put(srv, 0xF300, "b", tmp_path / "b")
    for off, n in [(0, 1), (0, MiB), (123, 4567), (MiB - 10, 20), (MiB // 2, 3 * MiB),
                   (size - 1, 1), (size - 100, 1000), (2 * MiB, -1)]:
        got = read_attachment(srv, doc, "b", offset=off, length=n)
        want = raw[off:] if n < 0 else raw[off:off + n]
        assert (got.get("size"), got.get("sha256")) == (len(want), sha(want)), (off, n, got)
    at_end = read_attachment(srv, doc, "b", offset=size, length=10)
    assert at_end.get("size") == 0, at_end
    past = read_attachment(srv, doc, "b", offset=size + 1, length=10)
    assert past.get("kind") == "range", past


def test_download_resumes_after_reconnect_on_the_same_version(srv, tmp_path):
    """A download loses its connection part way; meanwhile a new version commits. Reconnecting
    and resuming by range at the first read's commit completes the ORIGINAL version exactly."""
    v1 = mf.make_y4m(tmp_path / "v1.y4m", 70)  # ~8 MiB
    mf.make_y4m(tmp_path / "v2.y4m", 70, seed=3)
    doc = put(srv, 0xF400, "clip", tmp_path / "v1.y4m")
    ready, go = tmp_path / "ready", tmp_path / "go"
    result = {}

    def run():
        result.update(read_attachment(srv, doc, "clip", resume_at=3 * MiB + 77, pause_files=(ready, go)))

    t = threading.Thread(target=run)
    t.start()
    deadline = time.time() + 60
    while not ready.exists() and time.time() < deadline:
        time.sleep(0.05)
    assert ready.exists(), "the download never reached its disconnect point"
    new = attach(srv, doc, "clip", tmp_path / "v2.y4m")["commit"]
    go.write_text("go")
    t.join(timeout=120)
    assert (result.get("sha256"), result.get("size")) == (v1.sha256, v1.size), result
    assert result["resumed_commit"] == result["commit"] != new, result
    assert result["superseded"], f"the resumed read was not told a newer version exists: {result}"


def test_a_corrupted_chunk_inside_a_range_is_refused(srv, tmp_path):
    """A byte flipped on disk inside the requested range: the ranged read fails as corrupt (its
    inclusion proof cannot match), and a range elsewhere in the file still reads fine."""
    mf.make_bin(tmp_path / "b", 6 * MiB, seed=11)
    raw = (tmp_path / "b").read_bytes()
    doc = put(srv, 0xF500, "b", tmp_path / "b")
    srv.stop()
    seg = max(attachment_dir(srv).glob("u-*"), key=lambda f: f.stat().st_size)
    data = bytearray(seg.read_bytes())
    assert len(data) == len(raw), "expected the bytes segment to hold the file as-is"
    data[4 * MiB + 500] ^= 0xFF
    seg.write_bytes(bytes(data))
    srv.restart()
    bad = read_attachment(srv, doc, "b", offset=4 * MiB, length=1000)
    assert bad.get("kind") == "corrupt", f"a corrupt range was not refused as corrupt: {bad}"
    good = read_attachment(srv, doc, "b", offset=MiB, length=2 * MiB)
    assert good.get("sha256") == sha(raw[MiB:3 * MiB]), good
