"""Attachments across real nodes (docs/kdb-attachments-e2e-test-plan.md, series S).

test_attachments.py covers the basic two-node case; these add the invariant that matters most for
replicas - an attachment is never visible on a node before every byte of it is there - plus
writers on both sides, and a replica killed mid-transfer.
"""

from __future__ import annotations

import time

import pytest

import media_fixtures as mf
from attachment_fixtures import attach, doc_id, list_attachments, read_attachment, wait_for
from cluster_fixtures import Cluster, head
from server_fixtures import KdbServer, upsert

pytestmark = [pytest.mark.server, pytest.mark.cluster]

MiB = 1024 * 1024


def pull_from(src: KdbServer, name: str, interval: str = "300ms") -> list[str]:
    return ["--peer", f"name={name},addr={src.peer_addr},mode=pull,interval={interval},"
                      f"namespaces={src.namespace}"]


def diagnose(*nodes: KdbServer):
    return lambda: "\n".join(f"--- {n.sql_addr} head {head(n)}\n{n.logs()[-4000:]}" for n in nodes)


def test_a_replica_never_shows_an_attachment_before_it_has_every_byte(tmp_path):
    """S2: while B pulls a large attachment from A, B is polled continuously; the first moment B
    lists the attachment, B must already serve all of it, verified."""
    big = mf.make_bin(tmp_path / "big.bin", 48 * MiB + 7)
    a = KdbServer(control=True).start()
    b = None
    try:
        doc = doc_id(0x5200)
        upsert(a, doc, {"id": doc, "title": "big"})
        b = KdbServer(control=True, extra_args=pull_from(a, "a", "200ms")).start()
        wait_for("B to have the record", lambda: head(b) == head(a), on_fail=diagnose(a, b))
        attach(a, doc, "big.bin", tmp_path / "big.bin", timeout=300)
        polls, deadline = 0, time.time() + 300
        while time.time() < deadline:
            polls += 1
            listing = list_attachments(b, doc).get("attachments") or []
            if any(x["name"] == "big.bin" for x in listing):
                got = read_attachment(b, doc, "big.bin", timeout=300)
                assert got.get("sha256") == big.sha256, \
                    f"B listed the attachment before it could serve it (poll {polls}): {got}"
                break
            time.sleep(0.05)
        else:
            raise AssertionError("B never received the attachment\n" + diagnose(a, b)())
        assert polls > 1, "B had the attachment at the first poll - the race was not exercised"
    finally:
        if b is not None:
            b.stop()
        a.stop()


def test_writers_on_both_nodes_converge_with_every_attachment(tmp_path):
    """Two nodes syncing both ways; each attaches media to its own records, and one record gets
    an attachment from each side. Both end with every attachment, byte-identical and decodable."""
    files = {"a.y4m": mf.make_y4m(tmp_path / "a.y4m", 30), "a.wav": mf.make_wav(tmp_path / "a.wav", 2.0),
             "b.mp4": mf.make_mp4(tmp_path / "b.mp4", 2), "b.png": mf.make_png(tmp_path / "b.png")}
    c = Cluster(size=2).start()
    a, b = c.nodes
    try:
        shared, only_a, only_b = doc_id(0x5300), doc_id(0x5301), doc_id(0x5302)
        upsert(a, shared, {"id": shared, "title": "shared"})
        c.wait_converged()
        upsert(a, only_a, {"id": only_a})
        attach(a, only_a, "a.y4m", tmp_path / "a.y4m")
        attach(a, shared, "a.wav", tmp_path / "a.wav")
        upsert(b, only_b, {"id": only_b})
        attach(b, only_b, "b.mp4", tmp_path / "b.mp4")
        attach(b, shared, "b.png", tmp_path / "b.png")
        want = {(only_a, "a.y4m"), (shared, "a.wav"), (only_b, "b.mp4"), (shared, "b.png")}

        def has_all(n: KdbServer) -> bool:
            return all(read_attachment(n, d, name).get("sha256") == files[name].sha256 for d, name in want)

        wait_for("both nodes to hold every attachment", lambda: has_all(a) and has_all(b),
                 timeout=120, on_fail=lambda: c.diagnose())
        for n in (a, b):
            assert sorted(x["name"] for x in list_attachments(n, shared)["attachments"]) == ["a.wav", "b.png"]
            for d, name in want:
                out = tmp_path / f"{n.sql_port}-{name}"
                read_attachment(n, d, name, out=out)
                assert mf.truth_of(files[name].kind, out).same_media(files[name]), (n.sql_addr, name)
    finally:
        c.shutdown()


@pytest.mark.destructive
def test_replica_killed_mid_transfer_recovers_and_converges(tmp_path):
    """S5: SIGKILL the pulling node while it fetches a large attachment; after restart it
    finishes the transfer and serves the exact bytes."""
    big = mf.make_bin(tmp_path / "big.bin", 96 * MiB + 1)
    a = KdbServer(control=True).start()
    b = None
    try:
        doc = doc_id(0x5500)
        upsert(a, doc, {"id": doc})
        attach(a, doc, "big.bin", tmp_path / "big.bin", timeout=300)
        b = KdbServer(control=True, extra_args=pull_from(a, "a", "200ms")).start()
        from attachment_fixtures import stored_bytes
        deadline = time.time() + 120
        while stored_bytes(b) < 8 * MiB and time.time() < deadline:
            time.sleep(0.02)
        assert stored_bytes(b) < big.size, "B fetched everything before the kill - use a bigger file"
        b.kill9()
        b.restart()
        wait_for("B to converge after restart", lambda: head(b) == head(a), timeout=180,
                 on_fail=diagnose(a, b))
        got = read_attachment(b, doc, "big.bin", timeout=300)
        assert got.get("sha256") == big.sha256, got
    finally:
        if b is not None:
            b.stop()
        a.stop()
