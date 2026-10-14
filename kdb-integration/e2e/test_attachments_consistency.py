"""Attachment consistency under concurrency and crashes (docs/kdb-attachments-e2e-test-plan.md,
series P and C).

The owner's rule (docs/kdb-attachments-plan.md, decision 8): bytes are streamed from storage, a
read follows the same snapshot as the JSON read and pins the version it started on, a send in
progress finishes on that version even if a new one commits, the old version is disposed of only
after the read completes, and the reader is told a newer version may exist.
"""

from __future__ import annotations

import subprocess
import threading
import time

import pytest

import media_fixtures as mf
from attachment_fixtures import (
    PausedRead, attach, doc_id, list_attachments, maintained_server, read_attachment, stored_bytes,
    wait_stored_settles)
from server_fixtures import KdbServer, inspect_bin, upsert

pytestmark = [pytest.mark.server]

MiB = 1024 * 1024


@pytest.fixture
def history_none(monkeypatch):
    """Servers started in this test keep no history beyond the head, so a replaced attachment
    version becomes garbage as soon as maintenance truncates past it."""
    monkeypatch.setenv("KDB_HISTORY_MODE", "none")
    monkeypatch.setenv("KDB_RETAIN_COMMITS", "1")


# No server setting lets a short test reach GC of a replaced version: AttachmentHistoryLatest and
# the read lease are embed-only settings (no flag or env), and history=none truncates whole delta
# segments, which a test of a few commits never fills. The pinning itself is covered above (v1 is
# held and served intact while maintenance runs); "freed after the last reader closes" waits on a
# setting that makes v1 collectable.
COLLECT_NOT_REACHABLE = pytest.mark.xfail(
    strict=True, reason="no service setting makes a replaced attachment version collectable in a "
                        "short test (attachment history / read lease are not exposed)")


def test_a_read_finishes_on_its_version_while_a_new_one_commits_and_the_old_is_collected(
        tmp_path, history_none):
    """P1 + P2: a download of v1 pauses part way; v2 commits and maintenance runs until v1 is
    unreferenced. The download still completes with v1's exact bytes and reports that it was
    superseded; v1's segments are freed only after the reader closes."""
    v1 = mf.make_y4m(tmp_path / "v1.y4m", 60)
    v2 = mf.make_y4m(tmp_path / "v2.y4m", 60, seed=2)
    srv = maintained_server().start()
    try:
        doc = doc_id(0xE100)
        upsert(srv, doc, {"id": doc})
        attach(srv, doc, "clip", tmp_path / "v1.y4m")
        quiet = read_attachment(srv, doc, "clip")
        assert quiet["sha256"] == v1.sha256 and not quiet["superseded"], quiet

        read = PausedRead(srv, doc, "clip", pause_after=MiB, workdir=tmp_path, out=tmp_path / "got.y4m")
        attach(srv, doc, "clip", tmp_path / "v2.y4m")
        for i in range(5):  # move the head on so retention leaves v1 behind
            upsert(srv, doc_id(0xE1F0 + i), {"n": i})
        time.sleep(4)       # several maintenance passes while the read is paused
        held = stored_bytes(srv)
        assert held >= v1.size + v2.size, f"v1 was freed under an open reader ({held} bytes left)"

        got = read.resume()
        assert (got["sha256"], got["size"]) == (v1.sha256, v1.size), got
        assert mf.y4m_truth(tmp_path / "got.y4m").same_media(v1)
        assert got["superseded"], f"the reader was not told a newer version exists: {got}"
        now = read_attachment(srv, doc, "clip")
        assert now["sha256"] == v2.sha256 and not now["superseded"], now
    finally:
        srv.stop()


@COLLECT_NOT_REACHABLE
def test_the_old_version_is_freed_once_its_last_reader_closes(tmp_path, history_none):
    """P1, second half: after the pinned read of v1 closes, maintenance frees v1's segments."""
    v1 = mf.make_bin(tmp_path / "v1", 6 * MiB, seed=1)
    v2 = mf.make_bin(tmp_path / "v2", 6 * MiB, seed=2)
    srv = maintained_server().start()
    try:
        doc = doc_id(0xE150)
        upsert(srv, doc, {"id": doc})
        attach(srv, doc, "f", tmp_path / "v1")
        read = PausedRead(srv, doc, "f", pause_after=MiB, workdir=tmp_path)
        attach(srv, doc, "f", tmp_path / "v2")
        for i in range(5):
            upsert(srv, doc_id(0xE15F + i), {"n": i})
        assert read.resume()["sha256"] == v1.sha256
        settled = wait_stored_settles(srv, below=v2.size + 2 * MiB, timeout=30)
        assert settled < v1.size + v2.size
    finally:
        srv.stop()


def test_concurrent_readers_of_one_version_share_it_safely(tmp_path, history_none):
    """Several paused readers of v1 while v2 replaces it and maintenance runs: each completes with
    v1's bytes, including the last, which is still open after the others close."""
    v1 = mf.make_bin(tmp_path / "v1", 5 * MiB + 3, seed=1)
    mf.make_bin(tmp_path / "v2", 5 * MiB + 9, seed=2)
    srv = maintained_server().start()
    try:
        doc = doc_id(0xE200)
        upsert(srv, doc, {"id": doc})
        attach(srv, doc, "f", tmp_path / "v1")
        readers = []
        for i in range(3):
            d = tmp_path / f"r{i}"
            d.mkdir()
            readers.append(PausedRead(srv, doc, "f", pause_after=(i + 1) * MiB, workdir=d))
        attach(srv, doc, "f", tmp_path / "v2")
        for i in range(3):
            upsert(srv, doc_id(0xE2F0 + i), {"n": i})
        time.sleep(3)
        for r in readers[:-1]:
            got = r.resume()
            assert got["sha256"] == v1.sha256 and got["superseded"], got
        time.sleep(2)  # maintenance again, with one reader still holding v1
        got = readers[-1].resume()
        assert got["sha256"] == v1.sha256 and got["superseded"], got
    finally:
        srv.stop()


def test_a_finished_upload_survives_maintenance_until_it_is_committed(tmp_path):
    """Upload, let several maintenance passes run, then commit: the blob must still be there."""
    truth = mf.make_bin(tmp_path / "f", 3 * MiB, seed=42)
    srv = maintained_server().start()
    try:
        doc = doc_id(0xE300)
        upsert(srv, doc, {"id": doc})
        attach(srv, doc, "f", tmp_path / "f", commit_delay=2.0)
        assert read_attachment(srv, doc, "f")["sha256"] == truth.sha256
    finally:
        srv.stop()


def test_upload_grace_shorter_than_the_gap_loses_the_upload_negative_control(tmp_path):
    """The control for the test above: with --attachment-upload-grace below the gap between upload
    and commit, maintenance does collect the upload and the commit is refused - so the test above
    really exercises the gap, and the grace is what closes it."""
    mf.make_bin(tmp_path / "f", 3 * MiB, seed=43)
    srv = maintained_server(extra_args=["--attachment-upload-grace", "300ms"]).start()
    try:
        doc = doc_id(0xE310)
        upsert(srv, doc, {"id": doc})
        with pytest.raises(AssertionError, match="attachment blob not stored"):
            attach(srv, doc, "f", tmp_path / "f", commit_delay=3.0)
    finally:
        srv.stop()


# -- C: crashes ------------------------------------------------------------------------------

@pytest.mark.destructive
def test_kill9_mid_upload_leaves_nothing_visible_and_the_partial_upload_is_collected(tmp_path):
    """C1: SIGKILL while a large upload streams. After restart no record references it, the data
    dir verifies clean, and maintenance removes the partial upload's segments."""
    p = tmp_path / "big.bin"
    mf.make_bin(p, 96 * MiB)
    srv = maintained_server().start()
    try:
        doc = doc_id(0xC100)
        upsert(srv, doc, {"id": doc})
        up = subprocess.Popen(
            [__import__("server_fixtures").helper_bin(), "attach", "--addr", srv.sql_addr,
             "--namespace", srv.namespace, "--doc-id", doc, "--name", "big", "--file", str(p),
             "--timeout", "300s"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + 60
        while stored_bytes(srv) < 16 * MiB and time.time() < deadline and up.poll() is None:
            time.sleep(0.05)
        assert up.poll() is None, "the upload finished before the kill - make the file bigger"
        srv.kill9()
        up.communicate(timeout=60)

        r = subprocess.run([inspect_bin(), "verify", "--data-dir", srv.data_dir, "--namespace",
                            srv.namespace, "--codec", "zstd"], capture_output=True, text=True, timeout=120)
        assert r.returncode == 0, f"verify failed after kill -9:\n{r.stdout}\n{r.stderr}"
        srv.restart()
        assert list_attachments(srv, doc).get("attachments") in (None, []), "a half upload became visible"
        wait_stored_settles(srv, below=MiB, timeout=60)
    finally:
        srv.stop()


@pytest.mark.destructive
def test_kill9_during_sustained_attachment_writes_loses_nothing_acknowledged(tmp_path):
    """C3: a writer replaces attachments on a handful of records; SIGKILL at an arbitrary moment.
    Every acknowledged (record, name) reads back after restart as the last acknowledged bytes."""
    files = []
    for i in range(6):
        p = tmp_path / f"f{i}.bin"
        files.append((p, mf.make_bin(p, (i % 3) * MiB + 200_000 + i, seed=100 + i)))
    srv = KdbServer().start()
    acked: dict[tuple[str, str], int] = {}  # key -> index in attempts[key] of its last ack
    writes = [0]
    # Every value written to a key, in order, acknowledged or not: a write the kill cut off may
    # still have committed, so a key may legitimately read as any value attempted after its last
    # acknowledged one - never as an older one, and never as something never written.
    attempts: dict[tuple[str, str], list[str]] = {}
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            doc, name = doc_id(0xC300 + i % 4), f"a{(i // 4) % 2}"  # 8 keys
            path, truth = files[i % len(files)]
            attempts.setdefault((doc, name), []).append(truth.sha256)
            try:
                upsert(srv, doc, {"id": doc, "i": i}, timeout=10)
                res = attach(srv, doc, name, path, timeout=20)
            except (AssertionError, subprocess.TimeoutExpired):
                return
            if len(res.get("commit", "")) == 64:
                acked[(doc, name)] = len(attempts[(doc, name)]) - 1
                writes[0] += 1
            i += 1

    try:
        t = threading.Thread(target=writer)
        t.start()
        deadline = time.time() + 60
        while writes[0] < 20 and time.time() < deadline:
            time.sleep(0.02)
        time.sleep(0.7)
        srv.kill9()
        stop.set()
        t.join(timeout=60)
        assert writes[0] >= 10, f"only {writes[0]} acknowledged attachment writes before the kill"
        snapshot = dict(acked)
        srv.restart()
        wrong = {}
        for key, last_ack in snapshot.items():
            got = read_attachment(srv, *key)
            allowed = attempts[key][last_ack:]
            if got.get("sha256") not in allowed:
                wrong[key] = {"read": got, "allowed": allowed}
        assert not wrong, f"acknowledged attachments lost or wrong after kill -9: {wrong}"
    finally:
        srv.stop()
