"""Large attachments (docs/kdb-attachments-e2e-test-plan.md, size classes large and xl).

Decision 3 of docs/kdb-attachments-plan.md: no size limit, and bytes are never held whole. These
attach a file far bigger than any buffer, stream it back, and watch the server's resident memory
while they do: it must stay flat, not grow with the file.

slow: 1 GiB (run with -m slow). xl: 5 GiB, above every 32-bit size (run with -m xl; needs about
3x the file size free on the temp volume).
"""

from __future__ import annotations

import shutil
import subprocess
import threading

import pytest

import media_fixtures as mf
from attachment_fixtures import attach, doc_id, read_attachment, stored_bytes
from server_fixtures import KdbServer, upsert

pytestmark = [pytest.mark.server]

MiB, GiB = 1024 * 1024, 1024 * 1024 * 1024


class RssWatch(threading.Thread):
    """Samples a process's resident set size (KiB, from ps) until stopped."""

    def __init__(self, pid: int):
        super().__init__(daemon=True)
        self.pid, self.samples, self.stop_ev = pid, [], threading.Event()

    def run(self) -> None:
        while not self.stop_ev.wait(0.5):
            r = subprocess.run(["ps", "-o", "rss=", "-p", str(self.pid)], capture_output=True, text=True)
            if r.stdout.strip():
                self.samples.append(int(r.stdout.strip()) * 1024)

    def stop(self) -> int:
        self.stop_ev.set()
        self.join(timeout=5)
        return max(self.samples, default=0)


def round_trip_large(tmp_path, size: int, rss_limit: int) -> None:
    free = shutil.disk_usage(tmp_path).free
    if free < 3 * size + GiB:
        pytest.skip(f"needs {3 * size + GiB} bytes free on the temp volume, have {free}")
    src = tmp_path / "large.bin"
    truth = mf.make_bin(src, size, seed=size % 997)
    srv = KdbServer().start()
    try:
        doc = doc_id(0x1A00)
        upsert(srv, doc, {"id": doc, "title": "large"})
        baseline = int(subprocess.run(["ps", "-o", "rss=", "-p", str(srv.proc.pid)],
                                      capture_output=True, text=True).stdout.strip()) * 1024
        watch = RssWatch(srv.proc.pid)
        watch.start()
        timeout = max(600, size // (10 * MiB))
        info = attach(srv, doc, "large.bin", src, timeout=timeout)
        assert info["size"] == truth.size, info
        assert stored_bytes(srv) >= truth.size
        got = read_attachment(srv, doc, "large.bin", timeout=timeout)
        peak = watch.stop()
        assert (got["sha256"], got["size"]) == (truth.sha256, truth.size), got
        grew = peak - baseline
        print(f"RSS baseline {baseline // MiB} MiB, peak {peak // MiB} MiB moving {size // MiB} MiB")
        assert grew < rss_limit, (f"server RSS grew by {grew // MiB} MiB moving a {size // MiB} MiB "
                                  f"attachment (baseline {baseline // MiB} MiB, peak {peak // MiB} MiB)")
        srv.stop()
        srv.restart()
        again = read_attachment(srv, doc, "large.bin", timeout=timeout)
        assert again["sha256"] == truth.sha256
    finally:
        srv.stop()


@pytest.mark.slow
def test_one_gib_attachment_round_trips_with_flat_server_memory(tmp_path):
    round_trip_large(tmp_path, GiB + 12345, rss_limit=256 * MiB)


@pytest.mark.xl
def test_five_gib_attachment_round_trips_with_flat_server_memory(tmp_path):
    round_trip_large(tmp_path, 5 * GiB + 1, rss_limit=256 * MiB)
