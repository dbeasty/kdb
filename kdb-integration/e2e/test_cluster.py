"""Multi-node e2e: real kdb-service clusters replicating with --peer under concurrent load.

The existing peer tests (test_multi_peer_sync.py, test_replicator.py) prove one write crosses
one link. These prove a cluster stays one database while every node takes writes at once, while
nodes crash, stop and rejoin, and across topologies where data has to hop. Every scenario ends on
`Cluster.wait_converged`: the same head commit on every node, identical documents, no conflicts.

The heavier ones are marked slow and run in the nightly job (`pytest -m slow`).
"""

from __future__ import annotations

import json
import random
import time

import pytest

from cluster_fixtures import (
    Cluster,
    Writer,
    assert_values_at_least,
    conflicts,
    control,
    documents,
    head,
    ns_path,
    owned_docs,
    uuid_of,
)
from server_fixtures import get, preserve_evidence, put, sql_exec, sql_query

pytestmark = [pytest.mark.server, pytest.mark.cluster]


@pytest.fixture
def make_cluster(request):
    clusters: list[Cluster] = []

    def _make(size: int, **kw) -> Cluster:
        c = Cluster(size, **kw)
        clusters.append(c)
        return c.start()

    yield _make
    rep = getattr(request.node, "rep_call", None)
    if rep is not None and rep.failed:
        for k, c in enumerate(clusters):
            where = preserve_evidence(f"{request.node.name}-{k}", c.nodes, c.diagnose())
            print(f"cluster evidence preserved at {where}")
    problems = []
    for c in clusters:
        problems += c.shutdown()
    assert not problems, "unclean shutdown:\n" + "\n".join(problems)


def merged(writers: list[Writer]) -> dict[str, int]:
    out: dict[str, int] = {}
    for w in writers:
        out.update(w.snapshot())
    return out


# -- convergence under concurrent load -------------------------------------------------------

def test_mesh_of_four_converges_under_concurrent_writers(make_cluster):
    """Four nodes, every one writing its own documents at the same time."""
    c = make_cluster(4)
    writers = [Writer(n, owned_docs(i, 12), f"n{i}", limit=36) for i, n in enumerate(c.nodes)]
    for w in writers:
        w.start()
    for w in writers:
        w.join(timeout=180)
    assert all(not w.is_alive() for w in writers), "a writer did not finish"

    c.wait_converged()
    acked = merged(writers)
    assert len(acked) == 48
    for i, n in enumerate(c.nodes):
        assert_values_at_least(n, acked, f"n{i}")
        assert len(documents(n)) == 48


@pytest.mark.parametrize("topology", ["chain", "star"])
def test_writes_hop_across_topologies(make_cluster, topology):
    """Data written at either end must cross every node in between - in a chain, n0's write
    reaches n3 only through n1 and n2's own replicators."""
    c = make_cluster(4, topology=topology)
    first, last = c.nodes[0], c.nodes[-1]
    a, b = owned_docs(0, 1)[0], owned_docs(3, 1)[0]
    put(first, a, {"id": a, "from": "first"})
    put(last, b, {"id": b, "from": "last"})
    c.wait_converged()
    for n in c.nodes:
        assert get(n, a) == {"id": a, "from": "first"}
        assert get(n, b) == {"id": b, "from": "last"}


def test_sql_table_and_rows_replicate(make_cluster):
    """A table created on one node, with rows inserted on every node, reads the same everywhere:
    schema replicates as well as documents."""
    c = make_cluster(3)
    sql_exec(c.nodes[0], "CREATE TABLE orders (sku TEXT, qty INT)")
    c.wait_converged()
    for i, n in enumerate(c.nodes):
        for k in range(4):
            sql_exec(n, f"INSERT INTO orders (sku, qty) VALUES ('n{i}-{k}', {i * 10 + k})")
    c.wait_converged()
    expected = None
    for n in c.nodes:
        rows = sorted(map(tuple, sql_query(n, "SELECT sku, qty FROM orders")))
        assert len(rows) == 12, rows
        expected = expected or rows
        assert rows == expected


# -- conflicts -------------------------------------------------------------------------------

def test_contended_documents_converge_under_last_write(make_cluster):
    """Three nodes writing the SAME documents at once. Under last-write every node must settle on
    the same value per document - one that some node actually wrote - with nothing queued."""
    c = make_cluster(3, conflict_policy="last-write")
    shared = owned_docs(9, 6)
    writers = [Writer(n, shared, f"n{i}", limit=18, value_base=(i + 1) * 1_000_000)
               for i, n in enumerate(c.nodes)]
    for w in writers:
        w.start()
    for w in writers:
        w.join(timeout=180)

    c.wait_converged()
    written = {(d, v) for w in writers for d, v in w.all_acked}
    for doc, preview in documents(c.nodes[0]).items():
        body = json.loads(preview)
        assert (body["id"], body["v"]) in written, f"{doc} holds a value nobody wrote: {body}"


def test_strict_conflict_is_queued_then_resolved_everywhere(make_cluster):
    """Under strict, a same-document divergence is queued, not silently resolved; resolving it
    on one node produces a merge commit that replicates, after which the cluster converges."""
    c = make_cluster(2, control_write=True)
    a, b = c.nodes
    doc = owned_docs(7, 1)[0]
    put(a, doc, {"id": doc, "v": 0})
    c.wait_converged()

    # Diverge for certain: each side writes while the other is down, so neither can have
    # synced the other's write before making its own.
    assert b.stop() == 0
    put(a, doc, {"id": doc, "v": 1, "side": "a"})
    assert a.stop() == 0
    b.restart()
    put(b, doc, {"id": doc, "v": 2, "side": "b"})
    a.restart()

    deadline = time.time() + 60
    queued = []
    while time.time() < deadline and not queued:
        queued = [(n, q) for n in (a, b) for q in conflicts(n)]
        time.sleep(0.5)
    assert queued, "no conflict was queued for a same-document divergence:\n" + c.diagnose()

    node, conflict = queued[0]
    status, body = control(node, "POST", ns_path(node, f"/conflicts/{conflict['id']}/resolve"),
                           {"choices": {uuid_of(doc): {"take": "remote"}}})
    assert status in (200, 201), f"resolve failed ({status}): {body}"
    c.wait_converged()
    final = [get(n, doc) for n in (a, b)]
    assert final[0] == final[1] and final[0]["side"] in ("a", "b"), final


# -- crashes, partitions and restarts --------------------------------------------------------

@pytest.mark.destructive
def test_kill9_one_node_mid_replication_loses_nothing(make_cluster):
    """Three nodes under load; one is SIGKILLed and restarted twice while every node keeps
    writing. Afterwards each acknowledged write is on every node at its value or later."""
    c = make_cluster(3)
    writers = [Writer(n, owned_docs(i, 10), f"n{i}") for i, n in enumerate(c.nodes)]
    for w in writers:
        w.start()
    rng = random.Random(43)
    for _ in range(2):
        time.sleep(rng.uniform(1.5, 3.0))
        victim = c.nodes[rng.randrange(3)]
        victim.kill9()
        time.sleep(rng.uniform(0.5, 1.5))
        victim.restart()
    time.sleep(2)
    for w in writers:
        w.stop()

    c.wait_converged()
    acked = merged(writers)
    assert len(acked) == 30, f"only {len(acked)} documents ever acknowledged"
    for i, n in enumerate(c.nodes):
        assert_values_at_least(n, acked, f"n{i}")


@pytest.mark.destructive
def test_whole_cluster_kill9_at_once_loses_nothing(make_cluster):
    """Every node SIGKILLed in the same instant mid-load, then all restarted."""
    c = make_cluster(3)
    writers = [Writer(n, owned_docs(i, 8), f"n{i}") for i, n in enumerate(c.nodes)]
    for w in writers:
        w.start()
    time.sleep(3)
    for n in c.nodes:
        n.kill9()
    for w in writers:
        w.stop()
    acked = merged(writers)
    for n in c.nodes:
        n.restart()

    c.wait_converged()
    for i, n in enumerate(c.nodes):
        assert_values_at_least(n, acked, f"n{i}")


def test_stopped_node_catches_up_after_partition(make_cluster):
    """A node that is down while the others take writes catches up when it returns, and its
    own writes made right after rejoining reach everyone."""
    c = make_cluster(3)
    a, b, down = c.nodes
    down.stop()
    writers = [Writer(n, owned_docs(i, 10), f"n{i}", limit=20) for i, n in enumerate((a, b))]
    for w in writers:
        w.start()
    for w in writers:
        w.join(timeout=180)
    down.restart()
    late = owned_docs(2, 3)
    for d in late:
        put(down, d, {"id": d, "v": 99, "by": "n2"})

    c.wait_converged()
    acked = merged(writers) | {d: 99 for d in late}
    for i, n in enumerate(c.nodes):
        assert_values_at_least(n, acked, f"n{i}")


def test_full_cluster_restart_keeps_the_converged_state(make_cluster):
    """A clean stop and start of every node changes nothing: same head, same documents."""
    c = make_cluster(3)
    for i, n in enumerate(c.nodes):
        for d in owned_docs(i, 5):
            put(n, d, {"id": d, "by": f"n{i}"})
    before = c.wait_converged()
    docs_before = documents(c.nodes[0])
    for n in c.nodes:
        assert n.stop() == 0
    for n in c.nodes:
        n.restart()
    after = c.wait_converged()
    assert after == before, "a clean restart moved the head"
    for n in c.nodes:
        assert documents(n) == docs_before


@pytest.mark.slow
@pytest.mark.parametrize("peer_spec", [
    pytest.param("bootstrap=snapshot", marks=pytest.mark.xfail(strict=False, reason=(
        "a snapshot-bootstrapped node is stuck forever once a peer commits on a branch forked "
        "behind its shallow root: 'storing commit ...: missing parent ...' "
        "(found by this test; fixed by #111 - remove this mark once it lands)"))),
    "bootstrap=snapshot,deepen=true",
])
def test_late_joiner_bootstraps_from_a_snapshot_while_the_cluster_writes(make_cluster, peer_spec):
    """A fourth node joins a busy three-node mesh with bootstrap=snapshot and converges with it
    while writes continue on every original node. Concurrent writers make merges, so the
    snapshot root is usually a merge whose parents other nodes go on building on."""
    c = make_cluster(3)
    seed = owned_docs(5, 40)
    for d in seed:
        put(c.nodes[0], d, {"id": d, "v": 0, "by": "seed"})
    writers = [Writer(n, owned_docs(i, 6), f"n{i}") for i, n in enumerate(c.nodes)]
    for w in writers:
        w.start()
    time.sleep(1)
    joiner = c.add_node(peer_spec, peer_of=1)
    time.sleep(3)
    for w in writers:
        w.stop()

    c.wait_converged(timeout=60)
    acked = merged(writers) | {d: 0 for d in seed}
    assert_values_at_least(joiner, acked, "joiner")
    assert head(joiner) == head(c.nodes[0])


@pytest.mark.slow
@pytest.mark.destructive
@pytest.mark.xfail(strict=False, reason=(
    "a five-node mesh can fall into a merge storm and never converge: nodes keep creating empty "
    "merge commits of identical trees faster than they spread (10k+ seen after writes stopped; "
    "found by this test; fix in progress on fix/mesh-merge-convergence)"))
def test_rolling_kill9_soak(make_cluster):
    """Five nodes, all writing, for several rounds of SIGKILLing a random node - sometimes two
    at once - and bringing them back. Ends converged with every acknowledged write everywhere."""
    c = make_cluster(5)
    writers = [Writer(n, owned_docs(i, 8), f"n{i}") for i, n in enumerate(c.nodes)]
    for w in writers:
        w.start()
    rng = random.Random(4343)
    for _ in range(6):
        time.sleep(rng.uniform(1.0, 2.5))
        victims = rng.sample(c.nodes, rng.choice([1, 1, 2]))
        for v in victims:
            v.kill9()
        time.sleep(rng.uniform(0.3, 1.5))
        for v in victims:
            v.restart()
    time.sleep(2)
    for w in writers:
        w.stop()

    c.wait_converged(timeout=150)
    acked = merged(writers)
    for i, n in enumerate(c.nodes):
        assert_values_at_least(n, acked, f"n{i}")


def test_sql_updates_and_deletes_replicate(make_cluster):
    """UPDATE and DELETE made on different nodes reach every node: a deleted row must not come
    back from a peer that still had it, which is the case a value-based merge can get wrong."""
    c = make_cluster(3)
    a, b, d = c.nodes
    sql_exec(a, "CREATE TABLE items (name TEXT, n INT)")
    for k in range(9):
        sql_exec(c.nodes[k % 3], f"INSERT INTO items (name, n) VALUES ('item{k}', {k})")
    c.wait_converged()
    sql_exec(b, "UPDATE items SET n = 100 WHERE name = 'item1'")
    sql_exec(d, "DELETE FROM items WHERE name = 'item2'")
    # Bounded above: b's UPDATE may already have replicated to a, and n = 100 must not match.
    sql_exec(a, "DELETE FROM items WHERE n >= 7 AND n < 50")
    c.wait_converged()
    for n in c.nodes:
        rows = dict((r[0], int(r[1])) for r in sql_query(n, "SELECT name, n FROM items"))
        assert rows == {"item0": 0, "item1": 100, "item3": 3, "item4": 4, "item5": 5,
                        "item6": 6}, rows


@pytest.mark.xfail(strict=False, reason=(
    "about half the time the drained node checkpoints a peer commit it stored but never logged, "
    "and after restart fails every sync with 'commit ... has no frame in the delta log' "
    "(found by this test; fix in progress on fix/checkpoint-unlogged-peer-commits)"))
def test_graceful_drain_under_load_keeps_acknowledged_writes(make_cluster):
    """SIGTERM a node while every node is writing: it drains, exits 0, and nothing it
    acknowledged is lost once it is back."""
    c = make_cluster(3)
    writers = [Writer(n, owned_docs(i, 8), f"n{i}") for i, n in enumerate(c.nodes)]
    for w in writers:
        w.start()
    time.sleep(2)
    assert c.nodes[1].stop() == 0, f"drain under load did not exit cleanly:\n{c.nodes[1].logs()}"
    time.sleep(1)
    c.nodes[1].restart()
    time.sleep(1.5)
    for w in writers:
        w.stop()
    c.wait_converged()
    acked = merged(writers)
    for i, n in enumerate(c.nodes):
        assert_values_at_least(n, acked, f"n{i}")
