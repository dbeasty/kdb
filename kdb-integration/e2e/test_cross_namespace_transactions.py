"""E2e: transactions across namespaces (Layer 17 Phase E) against a real kdb-service.

The Go unit and crash tests drive NamespaceSet in process. These drive the shipped binary over
the wire: TX_COMMIT_MULTI from the Go client, SQL BEGIN/COMMIT routed by qualified table name,
and kill -9 in the middle of a stream of cross-namespace commits. The guarantee under test is
the one in docs/kdb-cross-namespace-transactions-plan.md §2: every namespace gets its part of a
transaction, or none does - including across a crash.
"""

from __future__ import annotations

import json
import subprocess
import time

import pytest

from server_fixtures import KdbServer, get, helper, helper_bin, inspect_bin, put, sql_query

pytestmark = pytest.mark.server

LEDGER = "e2e/ledger"
DOC_A = "000000000000000000000000000000a1"
DOC_B = "000000000000000000000000000000b1"


def get_in(server: KdbServer, ns: str, doc_id: str) -> dict | None:
    r = helper("get", "--addr", server.sql_addr, "--namespace", ns, "--doc-id", doc_id,
               check=False)
    if r.returncode != 0:
        return None
    lines = r.stdout.strip().splitlines()
    return json.loads(lines[1]) if len(lines) > 1 and lines[1] else None


def commit_across(server: KdbServer, parts: list[dict]) -> tuple[int, dict]:
    r = helper("commit-across", "--addr", server.sql_addr, "--parts", json.dumps(parts),
               check=False)
    assert r.returncode in (0, 3), f"helper failed ({r.returncode}):\n{r.stdout}\n{r.stderr}"
    return r.returncode, json.loads(r.stdout)


def test_commit_across_writes_every_namespace(server):
    code, out = commit_across(server, [
        {"namespace": server.namespace, "writes": {DOC_A: {"balance": 70}}},
        {"namespace": LEDGER, "writes": {DOC_B: {"debit": 30}}},
    ])
    assert code == 0, out
    assert set(out["commits"]) == {server.namespace, LEDGER}
    assert all(len(h) == 64 for h in out["commits"].values())
    assert get(server, DOC_A) == {"balance": 70}
    assert get_in(server, LEDGER, DOC_B) == {"debit": 30}


def test_conflict_in_one_namespace_writes_nothing_anywhere(server):
    base_a = put(server, DOC_A, {"balance": 100})
    stale_b = helper("put", "--addr", server.sql_addr, "--namespace", LEDGER,
                     "--doc-id", DOC_B, "--json", json.dumps({"debit": 0})).stdout.strip()
    # Someone else moves the ledger document on after we read it.
    helper("put", "--addr", server.sql_addr, "--namespace", LEDGER,
           "--doc-id", DOC_B, "--json", json.dumps({"debit": 5}))

    code, out = commit_across(server, [
        {"namespace": server.namespace, "base": base_a, "writes": {DOC_A: {"balance": 70}}},
        {"namespace": LEDGER, "base": stale_b, "writes": {DOC_B: {"debit": 30}}},
    ])
    assert code == 3, out
    assert out["refused"] == LEDGER and out["conflict"], out
    # The namespace that had no conflict must not have kept its part.
    assert get(server, DOC_A) == {"balance": 100}
    assert get_in(server, LEDGER, DOC_B) == {"debit": 5}


def test_sql_transaction_commits_and_rolls_back_across_namespaces(server):
    stmts = ["INSERT INTO t (owner, balance) VALUES ('ada', 70)",
             "INSERT INTO e2e.ledger (owner, debit) VALUES ('ada', 30)"]

    # A rolled-back transaction leaves no trace in either namespace.
    args = ["sql-tx", "--addr", server.sql_addr, "--namespace", server.namespace]
    for s in stmts:
        args += ["--stmt", s]
    assert helper(*args, "--rollback").stdout.strip() == "rolled-back"
    assert sql_query(server, "SELECT owner FROM t") == []
    assert sql_query(server, "SELECT owner FROM e2e.ledger") == []

    # A committed one lands in both.
    commit = helper(*args, "--snapshot").stdout.strip()
    assert len(commit) == 64
    assert sql_query(server, "SELECT owner, balance FROM t") == [["ada", "70"]]
    ledger = helper("query", "--addr", server.sql_addr, "--namespace", LEDGER,
                    "--sql", "SELECT owner, debit FROM ledger")
    assert json.loads(ledger.stdout)["rows"] == [["ada", "30"]]


@pytest.mark.destructive
def test_kill9_mid_stream_never_splits_a_transaction():
    """Kill the server while cross-namespace commits are in flight.

    Each round writes {"seq": i} to the same document in both namespaces as one transaction.
    After restart both namespaces must hold the same seq (no half-applied group), and that seq
    is at least the last one the client saw acknowledged (no acknowledged group lost).
    """
    srv = KdbServer().start()
    namespaces = [srv.namespace, LEDGER]
    try:
        proc = subprocess.Popen(
            [helper_bin(), "xns-transfers", "--addr", srv.sql_addr,
             "--namespaces", ",".join(namespaces), "--doc-id", DOC_A,
             "--rounds", "100000", "--timeout", "60s"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        acked = 0
        deadline = time.time() + 20
        assert proc.stdout is not None
        while acked < 40 and time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                break
            if line.startswith("acked "):
                acked = int(line.split()[1])
        srv.kill9()
        # Drain whatever was acknowledged before the kill reached the client.
        rest, _ = proc.communicate(timeout=30)
        for line in rest.splitlines():
            if line.startswith("acked "):
                acked = int(line.split()[1])
        assert acked >= 10, f"only {acked} acknowledged cross-namespace commits before the kill"

        for ns in namespaces:
            r = subprocess.run(
                [inspect_bin(), "verify", "--data-dir", srv.data_dir, "--namespace", ns,
                 "--codec", "zstd"],
                capture_output=True, text=True, timeout=60)
            assert r.returncode == 0, f"verify {ns} failed after kill -9:\n{r.stdout}\n{r.stderr}"

        srv.restart()
        seqs = {ns: (get_in(srv, ns, DOC_A) or {}).get("seq") for ns in namespaces}
        assert len(set(seqs.values())) == 1, f"a transaction was split by the crash: {seqs}"
        assert seqs[srv.namespace] >= acked, f"acknowledged seq {acked} lost: {seqs}"

        # And a second restart replays to the same answer (decisions are stable).
        assert srv.stop() == 0
        srv.restart()
        again = {ns: (get_in(srv, ns, DOC_A) or {}).get("seq") for ns in namespaces}
        assert again == seqs
        assert srv.stop() == 0
    finally:
        srv.cleanup()
