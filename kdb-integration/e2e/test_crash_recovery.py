"""E2e scenario 4 (Phase 3.2): crash recovery - kill -9 during sustained writes.

The layer-13 test plan's killDashNineAnyPoint, as a real process test: load writes, SIGKILL at
an arbitrary point, restart over the same data dir, run kdb-inspect verify, and confirm no
acknowledged write was lost. Acknowledged = the helper printed the commit hash before the kill.
"""

from __future__ import annotations

import subprocess
import threading
import time

import pytest

from server_fixtures import KdbServer, get, helper, inspect_bin, put

pytestmark = [pytest.mark.server, pytest.mark.destructive]


def test_kill9_mid_load_recovers_with_no_acknowledged_loss():
    srv = KdbServer().start()
    acked: list[tuple[str, int]] = []  # (doc_id, value) pairs whose put returned a commit hash
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            doc_id = f"{(i % 32) + 1:032x}"
            try:
                commit = put(srv, doc_id, {"id": doc_id, "v": i}, timeout=10)
            except (AssertionError, subprocess.TimeoutExpired):
                return  # the kill landed mid-request; unacked writes may vanish - that's fine
            if len(commit) == 64:
                acked.append((doc_id, i))
            i += 1

    try:
        t = threading.Thread(target=writer)
        t.start()
        # Let a meaningful burst of acknowledged writes build up, then pull the plug.
        deadline = time.time() + 10
        while len(acked) < 25 and time.time() < deadline:
            time.sleep(0.05)
        srv.kill9()
        stop.set()
        t.join(timeout=30)
        assert len(acked) >= 10, f"only {len(acked)} acknowledged writes before the kill"

        # The data dir must verify clean (torn tails are expected and tolerated by scan;
        # verify reports real corruption).
        r = subprocess.run(
            [inspect_bin(), "verify", "--data-dir", srv.data_dir,
             "--namespace", srv.namespace, "--codec", "zstd"],
            capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, f"verify failed after kill -9:\n{r.stdout}\n{r.stderr}"

        # Restart over the same directory: every acknowledged write's LAST value per doc must
        # be readable.
        srv.restart()
        final_by_doc: dict[str, int] = {}
        for doc_id, v in acked:
            final_by_doc[doc_id] = v
        for doc_id, v in final_by_doc.items():
            body = get(srv, doc_id)
            assert body is not None, f"acknowledged doc {doc_id} lost after kill -9"
            # The recovered value must be at least the last acknowledged one for that doc
            # (an unacked later write may also have survived - that's allowed).
            assert body["v"] >= v, f"doc {doc_id} rolled back past an acknowledged write: {body} < {v}"
        code = srv.stop()
        assert code == 0
    finally:
        stop.set()
        srv.cleanup()


def test_kill9_immediately_after_single_write_survives():
    srv = KdbServer().start()
    doc = "00000000000000000000000000000ee1"
    try:
        put(srv, doc, {"id": doc, "important": True})
        srv.kill9()
        srv.restart()
        body = get(srv, doc)
        assert body is not None and body["important"] is True
        code = srv.stop()
        assert code == 0
    finally:
        srv.cleanup()
