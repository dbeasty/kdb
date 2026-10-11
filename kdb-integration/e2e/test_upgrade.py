"""Mixed-version clusters: upgrading a v0.6.2 mesh to this build one node at a time.

Real deployments upgrade a node at a time, so for a while old and new nodes merge and sync with
each other. Since v0.6.2, main changed what a node does in a mesh:

- #114: two heads holding the same content are settled by one canonical "join" merge whose parents
  are the content commits beneath them, and main may move to a commit that does not descend from
  its old head (delta replay accepts a logged commit that `dag.SupersedesHead`).
- #111: a snapshot-bootstrapped node takes a fork point's state with SNAPSHOT_FETCH, and the
  serving node must fold that state from history - which a v0.6.2 server cannot do.
- #113: unlogged peer commits never reach a checkpoint; drain stops inbound peer sync first.
- #121: a log segment that cannot be scanned fails the open.

Every stage below ends on the strict convergence check (`Cluster.wait_converged`: the same head on
every node twice, identical documents, no conflicts), and every acknowledged write must be on
every node at its acknowledged value or later.

The previous release's binary comes from KDB_SERVICE_BIN_PREV (`make build-prev-service` builds
it from the v0.6.2 tag); without it every test here skips. They run nightly (`pytest -m upgrade`
in e2e-nightly.yml), not per PR.
"""

from __future__ import annotations

import random
import time

import pytest

from cluster_fixtures import (
    Cluster,
    Writer,
    assert_values_at_least,
    control,
    head,
    ns_path,
    owned_docs,
    peers,
)
from server_fixtures import (
    binary_version,
    preserve_evidence,
    prev_service_bin,
    put,
    sql_exec,
    sql_query,
)

PREV = prev_service_bin()

pytestmark = [
    pytest.mark.server,
    pytest.mark.cluster,
    pytest.mark.upgrade,
    pytest.mark.skipif(PREV is None, reason="KDB_SERVICE_BIN_PREV not set (make build-prev-service)"),
]

NEW = ""  # KdbServer.service_bin "" = this build's kdb-service


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
            notes = f"previous binary: {PREV} ({binary_version(PREV)})\n\n" + c.diagnose()
            where = preserve_evidence(f"{request.node.name}-{k}", c.nodes, notes)
            print(f"cluster evidence preserved at {where}")
    problems = []
    for c in clusters:
        problems += c.shutdown()
    assert not problems, "unclean shutdown:\n" + "\n".join(problems)


class Acked:
    """Every acknowledged write across a test's stages: the highest value per document. Each
    stage writes higher values than the last (value_base), so "at least" holds stage to stage."""

    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def add(self, writers: list[Writer]) -> None:
        for w in writers:
            for doc, v in w.snapshot().items():
                self.values[doc] = max(v, self.values.get(doc, v))

    def check(self, c: Cluster, stage: str) -> None:
        for i, n in enumerate(c.nodes):
            if n.proc is not None and n.proc.poll() is None:
                assert_values_at_least(n, self.values, f"{stage}: n{i} ({n.binary or 'new'})")


def load(c: Cluster, stage: int, seconds: float, during=None, docs_per_node: int = 6) -> list[Writer]:
    """Writers on every node (each its own documents) for `seconds`, calling `during()` halfway
    through - the step being tested happens under load. Returns the stopped writers."""
    writers = [Writer(n, owned_docs(i, docs_per_node), f"n{i}", value_base=stage * 100_000)
               for i, n in enumerate(c.nodes)]
    for w in writers:
        w.start()
    time.sleep(seconds / 2)
    if during is not None:
        during()
    time.sleep(seconds / 2)
    for w in writers:
        w.stop()
    return writers


def converge(c: Cluster, acked: Acked, stage: str, timeout: float = 90.0) -> str:
    try:
        h = c.wait_converged(timeout=timeout)
    except AssertionError as e:
        raise AssertionError(f"[{stage}] {e}") from None
    acked.check(c, stage)
    return h


# -- rolling upgrade -------------------------------------------------------------------------

@pytest.mark.parametrize("how", ["clean-stop", "kill9"])
def test_rolling_upgrade_of_a_three_node_mesh_under_load(make_cluster, how):
    """Three v0.6.2 nodes with a writer on each. One node at a time is taken down - a clean stop,
    or SIGKILL mid-load so that this build has to recover a log v0.6.2 crashed in - and brought
    back on this build with the same data dir and ports, while every node keeps writing. The mesh
    converges after each step, and at the end every node runs this build."""
    c = make_cluster(3, service_bins=[PREV] * 3)
    acked = Acked()
    acked.add(load(c, 0, 3))
    converge(c, acked, "all v0.6.2")

    for i in range(3):
        acked.add(load(c, i + 1, 4, during=lambda i=i: c.switch_binary(i, NEW, kill=(how == "kill9"))))
        converge(c, acked, f"n{i} upgraded ({how})")

    acked.add(load(c, 9, 3))
    converge(c, acked, "all new")
    assert all(n.service_bin == NEW for n in c.nodes)


# -- a long-lived mixed mesh -----------------------------------------------------------------

@pytest.mark.destructive
def test_five_node_mesh_two_old_three_new_with_kills(make_cluster):
    """Two v0.6.2 nodes and three of this build in one full mesh, every node writing, a few
    SIGKILLs (each victim comes back on the binary it ran). New nodes settle same-content heads
    with canonical joins, old ones with pairwise merges; the mesh has to end on one head.

    Two v0.6.2 nodes on their own can still produce a merge storm (N2, fixed by #114), but every
    head they make is also seen by three new nodes, which join it canonically."""
    c = make_cluster(5, service_bins=[PREV, PREV, NEW, NEW, NEW])
    acked = Acked()
    rng = random.Random(6262)

    def kills() -> None:
        for _ in range(3):
            victim = c.nodes[rng.randrange(5)]
            victim.kill9()
            time.sleep(rng.uniform(0.3, 1.0))
            victim.restart()
            time.sleep(rng.uniform(0.5, 1.5))

    acked.add(load(c, 1, 6, during=kills))
    converge(c, acked, "mixed 2 old + 3 new after kills", timeout=150)

    # Writes stopped: the mesh must stay on that head (no storm), then take more writes.
    time.sleep(5)
    assert len({head(n) for n in c.nodes}) == 1, "heads diverged after convergence:\n" + c.diagnose()
    acked.add(load(c, 2, 3))
    converge(c, acked, "mixed, second round", timeout=150)


# -- snapshot bootstrap from an old server ---------------------------------------------------

SHALLOW_TEXT = "behind this node's snapshot root"


def joiner_boundary_error(joiner) -> str | None:
    for p in peers(joiner).get("peers") or []:
        err = (p.get("State") or {}).get("lastError") or ""
        if SHALLOW_TEXT in err:
            return err
    return None


def snapshot_join_old_mesh(make_cluster, peer_spec: str):
    """A three-node v0.6.2 mesh under writes, joined mid-load by a node of this build with
    `peer_spec`, whose only peer is the old n1. Waits until the whole cluster converges or the
    joiner reports the typed shallow-boundary error; returns (cluster, joiner, acked, outcome,
    error text)."""
    c = make_cluster(3, service_bins=[PREV] * 3, control_write=True)
    acked = Acked()
    seed = owned_docs(5, 40)
    for d in seed:
        put(c.nodes[0], d, {"id": d, "v": 0, "by": "seed"})
    acked.values.update({d: 0 for d in seed})

    joined = {}

    def join() -> None:
        joined["node"] = c.add_node(peer_spec, peer_of=1, service_bin=NEW)

    acked.add(load(c, 1, 6, during=join))
    joiner = joined["node"]

    deadline = time.time() + 90
    outcome, err = None, None
    while time.time() < deadline and outcome is None:
        try:
            c.wait_converged(timeout=5)
            outcome = "converged"
        except AssertionError:
            err = joiner_boundary_error(joiner)
            if err:
                outcome = "shallow-boundary"
    print(f"snapshot join ({peer_spec}) from a v0.6.2 server: {outcome}" + (f"\n{err}" if err else ""))
    assert outcome is not None, (
        "the joiner neither converged nor reported the typed shallow-boundary error - a silent "
        "stall:\n" + c.diagnose())
    return c, joiner, acked, outcome, err


@pytest.mark.slow
def test_new_node_snapshot_joins_a_v062_server_while_the_cluster_writes(make_cluster):
    """A node of this build joins a busy three-node v0.6.2 mesh with bootstrap=snapshot, its only
    peer an old node. When the mesh forks behind the joiner's snapshot root it needs the fork
    point's state, which this build takes with SNAPSHOT_FETCH; a v0.6.2 server can serve that only
    for a commit whose tree it built, and the fork point is usually a commit it merged (it cannot
    fold one from history, #111). So the joiner either converges or reports the typed
    ShallowBoundaryError naming deepen - never a silent stall. In the error case the deepen the
    error names, through the control plane, recovers it."""
    c, joiner, acked, outcome, err = snapshot_join_old_mesh(make_cluster, "bootstrap=snapshot")
    if outcome == "shallow-boundary":
        assert "INTERNAL: no tree" in err, f"expected the old server's 'no tree':\n{err}"
        assert "deepen" in err, err
        status, body = control(joiner, "POST", ns_path(joiner, "/deepen"), {"peer": "n1"},
                               timeout=60)
        assert status == 200, f"deepen from the v0.6.2 peer failed ({status}): {body}"
    converge(c, acked, f"joiner after {outcome}", timeout=120)
    assert head(joiner) == head(c.nodes[0])


@pytest.mark.slow
def test_new_node_snapshot_joins_a_v062_server_with_deepen(make_cluster):
    """bootstrap=snapshot,deepen=true against a v0.6.2 server: the deepen right after the bootstrap
    fetches the history a fork point needs before the mesh can fork behind it."""
    c, joiner, acked, outcome, err = snapshot_join_old_mesh(
        make_cluster, "bootstrap=snapshot,deepen=true")
    assert outcome == "converged", err
    acked.check(c, "deepen joiner")


@pytest.mark.slow
@pytest.mark.xfail(strict=False, reason=(
    "the deepen=true peer option never runs once the sync fails: replication.peerLoop.deepen "
    "skips namespaces whose sync returned an error, and a ShallowBoundaryError is one - so the "
    "option the error message recommends cannot recover a joiner already stuck"))
def test_deepen_peer_option_recovers_a_stuck_snapshot_joiner(make_cluster):
    """The typed error says the deepen=true peer option recovers it. Restart a stuck joiner with
    that option added and see whether it does."""
    c, joiner, acked, outcome, err = snapshot_join_old_mesh(make_cluster, "bootstrap=snapshot")
    if outcome != "shallow-boundary":
        pytest.skip("the joiner converged without hitting the shallow boundary this run")
    assert joiner.stop() == 0
    joiner.extra_args = [a + ",deepen=true" if a.startswith("name=n1,") else a
                         for a in joiner.extra_args]
    joiner.restart()
    converge(c, acked, "stuck joiner restarted with deepen=true", timeout=60)


# -- SQL across versions ---------------------------------------------------------------------

def test_sql_schema_and_rows_from_old_nodes_read_and_extended_on_new(make_cluster):
    """A table and rows created on v0.6.2 nodes; one node upgraded, then both versions insert,
    update and delete; then the rest upgraded and the old rows changed on the new build. Every node
    reads the same rows at every stage. (One table: a namespace holds one SQL schema, so a second
    CREATE TABLE is refused on either version.)"""
    c = make_cluster(3, service_bins=[PREV] * 3)
    acked = Acked()
    sql_exec(c.nodes[0], "CREATE TABLE orders (sku TEXT, qty INT)")
    converge(c, acked, "schema on v0.6.2")
    for i, n in enumerate(c.nodes):
        for k in range(4):
            sql_exec(n, f"INSERT INTO orders (sku, qty) VALUES ('old{i}-{k}', {i * 10 + k})")
    converge(c, acked, "rows on v0.6.2")

    def rows(n) -> dict[str, int]:
        return {r[0]: int(r[1]) for r in sql_query(n, "SELECT sku, qty FROM orders")}

    expected = {f"old{i}-{k}": i * 10 + k for i in range(3) for k in range(4)}
    for n in c.nodes:
        assert rows(n) == expected

    c.switch_binary(0, NEW)
    converge(c, acked, "n0 upgraded")
    assert rows(c.nodes[0]) == expected, "the upgraded node does not read the old rows"
    sql_exec(c.nodes[0], "INSERT INTO orders (sku, qty) VALUES ('new0', 100)")
    sql_exec(c.nodes[1], "INSERT INTO orders (sku, qty) VALUES ('old1-late', 101)")
    sql_exec(c.nodes[0], "UPDATE orders SET qty = 500 WHERE sku = 'old1-0'")
    sql_exec(c.nodes[2], "DELETE FROM orders WHERE sku = 'old0-3'")
    converge(c, acked, "mixed SQL writes")
    expected |= {"new0": 100, "old1-late": 101, "old1-0": 500}
    del expected["old0-3"]
    for n in c.nodes:
        assert rows(n) == expected, n.binary

    c.switch_binary(1, NEW)
    c.switch_binary(2, NEW)
    converge(c, acked, "all new")
    for n in c.nodes:
        assert rows(n) == expected, n.binary
    for i, n in enumerate(c.nodes):
        sql_exec(n, f"INSERT INTO orders (sku, qty) VALUES ('new-n{i}', {200 + i})")
    sql_exec(c.nodes[1], "UPDATE orders SET qty = 600 WHERE sku = 'old2-1'")
    sql_exec(c.nodes[2], "DELETE FROM orders WHERE sku = 'old0-0'")
    converge(c, acked, "all new, rows")
    expected |= {f"new-n{i}": 200 + i for i in range(3)} | {"old2-1": 600}
    del expected["old0-0"]
    for n in c.nodes:
        assert rows(n) == expected


# -- rollback (downgrade is not supported; this records what happens) ------------------------

def _new_cluster_history(c: Cluster, acked: Acked) -> None:
    """Rounds of concurrent writes with convergence in between - the quiet after each round is
    when this build settles same-content heads with canonical joins (#114) - and then one more
    write per node on top of whatever main moved to."""
    for rnd in range(3):
        acked.add(load(c, rnd, 3))
        c.wait_converged()
    for i, n in enumerate(c.nodes):
        d = owned_docs(i, 6)[0]
        put(n, d, {"id": d, "v": 299_999, "by": "final"})
        acked.values[d] = 299_999
    converge(c, acked, "new build, before rollback")


def test_rollback_to_v062_after_a_clean_stop(make_cluster):
    """Data dirs a five-node mesh of this build wrote, stopped cleanly, open on v0.6.2 with every
    acknowledged write, and that mesh converges and takes writes. A clean stop leaves a checkpoint
    at main, so v0.6.2 has no log to replay."""
    c = make_cluster(5)
    acked = Acked()
    _new_cluster_history(c, acked)
    for n in c.nodes:
        assert n.stop() == 0
    for n in c.nodes:
        n.restart(service_bin=PREV)
    acked.check(c, "rolled back to v0.6.2 (clean)")
    acked.add(load(c, 7, 3))
    converge(c, acked, "v0.6.2 after rollback")


@pytest.mark.destructive
@pytest.mark.xfail(strict=False, reason=(
    "rollback after a crash loses acknowledged writes: v0.6.2's delta replay does not move main "
    "onto a logged canonical join that does not descend from it (#114 SupersedesHead), so every "
    "later local write is stored but not applied - and the mesh can converge on the old values"))
def test_rollback_to_v062_after_a_crash(make_cluster):
    """The same history, but every node SIGKILLed so that v0.6.2 must replay this build's log.

    Recorded, not a defect in this build: the same crashed data dirs open with every write on this
    build. Once v0.6.2 has stopped cleanly over such a dir, its checkpoint names the stale head and
    this build reopens there too, so the loss outlives rolling forward again."""
    c = make_cluster(5)
    acked = Acked()
    _new_cluster_history(c, acked)
    for n in c.nodes:
        n.kill9()
    for n in c.nodes:
        n.restart(service_bin=PREV)
    acked.check(c, "rolled back to v0.6.2 after kill -9, before sync")
    converge(c, acked, "rolled back to v0.6.2 after kill -9")


@pytest.mark.destructive
def test_rollback_after_a_crash_is_safe_once_this_build_recovers_and_stops_cleanly(make_cluster):
    """The safe way back from a crashed node: start it on this build first, so this build replays
    its own log, stop it cleanly (a checkpoint at main), and only then start v0.6.2."""
    c = make_cluster(5)
    acked = Acked()
    _new_cluster_history(c, acked)
    for n in c.nodes:
        n.kill9()
    for n in c.nodes:
        n.restart()
    acked.check(c, "recovered on this build")
    for n in c.nodes:
        assert n.stop() == 0
    for n in c.nodes:
        n.restart(service_bin=PREV)
    acked.check(c, "rolled back to v0.6.2 after recovering on this build")
    converge(c, acked, "v0.6.2 after a recovered rollback")
