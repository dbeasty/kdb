"""Resolution chains on a mesh: do merges of DIFFERENT content converge, and on the right value?

#114 made merges of same-content heads converge through a canonical join. Merges of different
content are still pairwise: a node merges two heads at a time, so three concurrent writes A, B, C
are settled as merge(merge(A,B),C) on one node and merge(merge(A,C),B) on another. Whether those
agree depends on each rule being associative and commutative over the values it is handed, and on
the base each pairwise merge is given. These scenarios put every chain rule
(docs/kdb-user-guide.md, "Resolution chains") under exactly that pressure on 3- and 5-node meshes.

Two kinds of test per rule:

* **Ordered** (default run). The concurrent writes are made on isolated nodes (restarted without
  --peer), then chosen nodes pull from one peer at a time (restarted with a single pull-only peer
  and a sync forced through the control plane), so the merge order is fixed rather than left to
  timing. Then the whole mesh is restarted with its normal --peer specs and must pass the strict
  `Cluster.wait_converged`, and the converged value is checked against what the rule says it
  should be.
* **Load** (`slow`, nightly). Every node writes the same documents at once through the control
  plane while one node is killed with SIGKILL and restarted; the same convergence and semantic
  checks follow.

Every chain here is set at version 2 (peersync.ChainVersionMergeBases): the merge semantics that
fix #130's F1 (wrong merge base), F2 (order-dependent field-merge then last-write) and F4 (validity
before the schema arrives). Version 1 chains keep the original behaviour for meshes with nodes on
older releases; test_upgrade.py covers that and the switch. What still fails is marked xfail with
the mechanism: F3, procedures that are not state-based joins, which is a documentation matter.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time

import pytest

from cluster_fixtures import Cluster, conflicts, control, documents, head, ns_path, tail
from server_fixtures import helper, preserve_evidence

pytestmark = [pytest.mark.server, pytest.mark.cluster]

SIZES = [3, 5]


# -- fixtures and plumbing ---------------------------------------------------------------------

@pytest.fixture
def make_cluster(request):
    clusters: list[Cluster] = []

    def _make(size: int, **kw) -> Cluster:
        kw.setdefault("control_write", True)
        c = Cluster(size, **kw)
        clusters.append(c)
        return c.start()

    yield _make
    rep = getattr(request.node, "rep_call", None)
    keep = rep is not None and (rep.failed or (
        hasattr(rep, "wasxfail") and os.environ.get("KDB_E2E_EVIDENCE_ON_XFAIL")))
    if keep:
        for k, c in enumerate(clusters):
            where = preserve_evidence(f"{request.node.name}-{k}", c.nodes, c.diagnose())
            print(f"cluster evidence preserved at {where}")
    problems = []
    for c in clusters:
        problems += c.shutdown()
    assert not problems, "unclean shutdown:\n" + "\n".join(problems)


def doc_uuid(n: int) -> str:
    return f"00000000-0000-4000-8000-{0xc0f000 + n:012x}"


def wait_for(what: str, cond, timeout: float = 30.0, diag=None) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}" + (f"\n{diag()}" if diag else ""))


def node_id(n) -> str:
    status, body = control(n, "GET", ns_path(n, "/resolution"))
    assert status == 200, body
    return body["thisNode"]


def chain_hash(n) -> str | None:
    status, body = control(n, "GET", ns_path(n, "/resolution"))
    return body.get("hash") if status == 200 else None


# KDB_E2E_CHAIN_VERSION=1 reruns the file under the original semantics, which is how the F1, F2
# and F4 scenarios are confirmed to still catch the defects they were written for.
CHAIN_VERSION = int(os.environ.get("KDB_E2E_CHAIN_VERSION", "2"))


def set_chain(c: Cluster, rules: list[dict], on: int = 0, version: int = CHAIN_VERSION) -> str:
    """PUTs the chain on one node and waits for every node to hold it (through _kdb/meta)."""
    status, body = control(c.nodes[on], "PUT", ns_path(c.nodes[on], "/resolution"),
                           {"rules": rules, "version": version})
    assert status == 200, f"PUT resolution failed ({status}): {body}"
    want = body["hash"]
    wait_for("the chain to replicate to every node",
             lambda: all(chain_hash(n) == want for n in c.nodes), diag=c.diagnose)
    return want


def set_procedure(c: Cluster, name: str, source: str, on: int = 0) -> None:
    n0 = c.nodes[on]
    status, body = control(n0, "PUT", ns_path(n0, f"/procedures/{name}"), {"source": source})
    assert status in (200, 201), f"PUT procedure failed ({status}): {body}"

    def hashes(n):
        st, b = control(n, "GET", ns_path(n, "/procedures"))
        return json.dumps(b.get("procedures"), sort_keys=True) if st == 200 else None

    want = hashes(n0)
    wait_for("the procedure to replicate", lambda: all(hashes(n) == want for n in c.nodes),
             diag=c.diagnose)


def read_doc(n, doc: str) -> tuple[dict | None, str | None]:
    status, body = control(n, "GET", ns_path(n, f"/docs/{doc}"))
    if status == 404:
        return None, None
    assert status == 200, f"read {doc} on {n.sql_addr} failed ({status}): {body}"
    return body["body"], body["contentHash"]


def write(n, doc: str, fields: dict, if_hash: str | None = None, replace: bool = False) -> tuple[int, dict]:
    """A control-plane write: a top-level merge over the stored document (a patch), unless
    replace. if_hash makes it a compare-and-set."""
    req: dict = {"body": fields}
    if if_hash:
        req["ifContentHash"] = if_hash
    if replace:
        req["replace"] = True
    return control(n, "PUT", ns_path(n, f"/docs/{doc}"), req)


def must_write(n, doc: str, fields: dict) -> str:
    status, body = write(n, doc, fields)
    assert status == 200, f"write {fields} to {doc} on {n.sql_addr} failed ({status}): {body}"
    return body["commit"]


def cas_update(n, doc: str, fn, attempts: int = 50) -> dict | None:
    """Read-modify-write under compare-and-set; returns the stored body, or None on give-up."""
    for _ in range(attempts):
        try:
            body, h = read_doc(n, doc)
        except Exception:
            time.sleep(0.2)
            continue
        if body is None:
            return None
        new = fn(body)
        try:
            status, resp = write(n, doc, new, if_hash=h)
        except Exception:
            status, resp = 0, {}
        if status == 200:
            return {**body, **new, "_commit": resp["commit"]}
        time.sleep(0.01 if status == 409 else 0.2)
    return None


def all_docs(c: Cluster) -> list[dict[str, dict]]:
    return [{k: json.loads(v) for k, v in documents(n).items()} for n in c.running()]


def converged_docs(c: Cluster, timeout: float = 120.0) -> dict[str, dict]:
    c.wait_converged(timeout=timeout)
    return {k: json.loads(v) for k, v in documents(c.nodes[0]).items()}


def commit_ms(n, commit: str) -> int:
    status, body = control(n, "GET", ns_path(n, f"/commits/{commit}"))
    assert status == 200, body
    ts = body["commit"]["timestamp"]
    from datetime import datetime
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


def graph_excerpt(n, limit: int = 30) -> str:
    """The newest commits on n, for a failure message: hash, parents, author, ops."""
    status, body = control(n, "GET", ns_path(n, f"/log?limit={limit}"))
    if status != 200:
        return f"(log unreadable: {status} {body})"
    rows = []
    for cm in body.get("commits") or []:
        rows.append(f"   {cm['hash'][:10]} <- {','.join(p[:10] for p in cm.get('parents') or [])} "
                    f"author={cm.get('author', '')[:8]} ops={cm.get('opCount')} "
                    f"ts={cm.get('timestamp')} {cm.get('message', '')[:40]}")
    return "\n".join(rows)


# -- staging: isolated writes, then merges in a chosen order, then the mesh ---------------------

class Stages:
    """Relaunches a Cluster's nodes with different peer wiring over the same data directories.

    isolate():  every node without --peer, so writes stay local and are concurrent by
                construction.
    pull(i, j): node i relaunched with node j as its only peer, pull-only, then synced once. A
                pull-only peer never pushes, so j is left exactly as it was: the merge happens on
                i and nowhere else, in exactly the order the test asks for.
    mesh():     every node back on the Cluster's own --peer specs (bidirectional, short interval).
    """

    def __init__(self, c: Cluster):
        self.c = c
        self.mesh_args = [list(n.extra_args) for n in c.nodes]
        self.base_args = list(c.node_args) + ["--peer-conflict-policy", c.conflict_policy]
        self.log: list[str] = []

    def _stop(self, i: int) -> None:
        n = self.c.nodes[i]
        if n.proc is not None and n.proc.poll() is None:
            code = n.stop()
            assert code == 0, f"n{i} exited {code}:\n{tail(n.logs())}"

    def _launch(self, i: int, args: list[str]) -> None:
        n = self.c.nodes[i]
        n.extra_args = args
        n.restart()

    def isolate(self) -> None:
        for i in range(len(self.c.nodes)):
            self._stop(i)
        for i in range(len(self.c.nodes)):
            self._launch(i, list(self.base_args))
        self.log.append("isolate")

    def pull(self, i: int, j: int) -> dict:
        c = self.c
        before = head(c.nodes[i])
        self._stop(i)
        spec = f"name=n{j},addr={c.nodes[j].peer_addr},interval=1h,mode=pull"
        self._launch(i, list(self.base_args) + ["--peer", spec])
        status, body = control(c.nodes[i], "POST", f"/v1/peers/n{j}/sync", timeout=60)
        assert status == 200, f"n{i} pulling n{j} failed ({status}): {body}"
        # The relaunch syncs once by itself, so the forced sync may find nothing left to do; the
        # head before and after is what says what happened.
        after = head(c.nodes[i])
        self.log.append(f"n{i} <- n{j}: {str(before)[:10]} -> {str(after)[:10]} "
                        f"(peer at {str(head(c.nodes[j]))[:10]})")
        return body

    def mesh(self) -> None:
        for i in range(len(self.c.nodes)):
            self._stop(i)
        for i in range(len(self.c.nodes)):
            self._launch(i, list(self.mesh_args[i]))
        self.log.append("mesh")


def settle(c: Cluster, timeout: float = 60.0) -> None:
    """Waits for the cluster to converge after a definition change or a base write."""
    c.wait_converged(timeout=timeout)


# -- 1. field-merge, then last-write -----------------------------------------------------------

FIELD_MERGE_CHAIN = [{"kind": "field-merge"}, {"kind": "last-write"}]


@pytest.mark.parametrize("size", SIZES)
def test_field_merge_keeps_every_disjoint_field_change(make_cluster, size):
    """Every node changes a DIFFERENT top-level field of the same document while isolated. Merged
    in any order, field-merge must keep every change: the result is the same on every node and
    holds all of them."""
    c = make_cluster(size)
    set_chain(c, FIELD_MERGE_CHAIN)
    doc = doc_uuid(1)
    fields = [f"f{i}" for i in range(size)]
    must_write(c.nodes[0], doc, {"id": doc, **{f: 0 for f in fields}})
    settle(c)

    st = Stages(c)
    st.isolate()
    for i, n in enumerate(c.nodes):
        must_write(n, doc, {f"f{i}": f"n{i}"})
    # Two different association orders on two different nodes.
    st.pull(0, 1)
    st.pull(0, 2)          # n0 = merge(merge(A,B),C)
    st.pull(2, 1)          # n2 = merge(C,B)
    if size == 5:
        st.pull(3, 2)      # n3 = merge(D, merge(C,B))
        st.pull(4, 0)      # n4 = merge(E, merge(merge(A,B),C))
    st.mesh()

    final = converged_docs(c)[doc]
    want = {"id": doc, **{f"f{i}": f"n{i}" for i in range(size)}}
    assert final == want, f"field-merge lost a change: got {final}, want {want}\n{st.log}"


@pytest.mark.parametrize("size", SIZES)
def test_field_merge_collision_has_the_last_write_winner_everywhere(make_cluster, size):
    """n0 and then n1 write the SAME field x; afterwards n2 writes a different field c. Where the
    fields differ every change is kept (c); where they collide, last-write's winner is the later
    write - n1's - and it is the same on every node, whichever order the merges ran in.

    The merge order chosen here is the one that exposes the defect: n2 merges n0 first (a
    field-merge, so the value it builds is attributed to that merge commit, stamped with n2's
    later time), then n1 merges that, and its last-write compares n1's write against the merge's
    timestamp rather than n0's."""
    c = make_cluster(size)
    set_chain(c, FIELD_MERGE_CHAIN)
    doc = doc_uuid(2)
    must_write(c.nodes[0], doc, {"id": doc, "x": "base", "c": 0})
    settle(c)

    st = Stages(c)
    st.isolate()
    must_write(c.nodes[0], doc, {"x": "n0"})
    time.sleep(0.05)
    must_write(c.nodes[1], doc, {"x": "n1"})     # the later write of x
    time.sleep(0.05)
    must_write(c.nodes[2], doc, {"c": 2})         # a bystander field, written last

    order_evidence = ""
    if size == 5:
        # The two association orders side by side, on nodes that wrote nothing.
        st.pull(3, 0)
        st.pull(3, 2)
        st.pull(3, 1)      # n3 = merge(merge(A, C), B)
        st.pull(4, 1)
        st.pull(4, 2)
        st.pull(4, 0)      # n4 = merge(merge(B, C), A)
        d3, d4 = read_doc(c.nodes[3], doc)[0], read_doc(c.nodes[4], doc)[0]
        order_evidence = f"merge(merge(A,C),B)={d3} merge(merge(B,C),A)={d4}"
    st.pull(2, 0)          # n2 = merge(C, A): field-merge, attributed to the merge
    st.pull(1, 2)          # n1 = merge(B, merge(C, A)): x collides, last-write decides
    mid = read_doc(c.nodes[1], doc)[0]
    st.mesh()

    final = converged_docs(c)[doc]
    want = {"id": doc, "x": "n1", "c": 2}
    problems = []
    if size == 5 and not (d3 == d4 == want):
        # Deterministic: the same three writes, merged in two association orders, differ.
        problems.append(f"order-dependent: {order_evidence}, want {want} from both")
    if final != want:
        problems.append(f"converged on {final}, want {want} (n1 wrote x last); "
                        f"n1 after merging merge(C,A): {mid}")
    assert not problems, "\n".join(problems) + f"\n{st.log}\n" + graph_excerpt(c.nodes[0])


@pytest.mark.parametrize("size", SIZES)
def test_field_merge_against_the_true_merge_base(make_cluster, size):
    """Two heads whose real common history is A (a=1). Since A, one side changed a to 2 and b;
    the other changed c and e (and d is on both). Field-merge must keep all of it - a=2, b, c,
    d, e - because no field was changed differently by both sides since what they share. Built so
    the nearest common ancestor is several hops away while an older one (or a second nearest one)
    is closer, which is what the merge-base search gets wrong."""
    c = make_cluster(size)
    set_chain(c, FIELD_MERGE_CHAIN)
    doc = doc_uuid(3)
    must_write(c.nodes[0], doc, {"id": doc, "a": 0, "b": 0, "c": 0, "d": 0, "e": 0})
    settle(c)

    st = Stages(c)
    st.isolate()
    if size == 5:
        must_write(c.nodes[0], doc, {"a": 1})        # A
        st.pull(1, 0)
        st.pull(2, 0)                                # n1, n2 at A
        must_write(c.nodes[1], doc, {"b": 1})
        must_write(c.nodes[1], doc, {"a": 2})        # n1: A -> B1 -> B2
        must_write(c.nodes[2], doc, {"c": 1})
        must_write(c.nodes[2], doc, {"c": 2})        # n2: A -> C1 -> C2
        must_write(c.nodes[3], doc, {"d": 1})        # D, straight off the shared base
        must_write(c.nodes[4], doc, {"d": 1, "e": 1})  # E, likewise
        st.pull(1, 3)                                # H1 = merge(B2, D)
        st.pull(2, 4)                                # H2 = merge(C2, E)
    else:
        must_write(c.nodes[1], doc, {"a": 1})        # A
        st.pull(2, 1)                                # n2 at A
        must_write(c.nodes[0], doc, {"d": 1})        # D, off the shared base
        must_write(c.nodes[1], doc, {"b": 1})
        must_write(c.nodes[1], doc, {"a": 2})        # n1: A -> B1 -> B2
        must_write(c.nodes[2], doc, {"c": 1})
        must_write(c.nodes[2], doc, {"c": 2})        # n2: A -> C1 -> C2
        st.pull(1, 0)                                # H1 = merge(B2, D)
        must_write(c.nodes[0], doc, {"e": 1})        # E, on D
        st.pull(2, 0)                                # H2 = merge(C2, E)
    h1, h2 = read_doc(c.nodes[1], doc)[0], read_doc(c.nodes[2], doc)[0]
    st.pull(1, 2)                                    # merge(H1, H2)
    merged_once = read_doc(c.nodes[1], doc)[0]
    st.mesh()

    final = converged_docs(c)[doc]
    want = {"id": doc, "a": 2, "b": 1, "c": 2, "d": 1, "e": 1}
    assert final == want, (
        f"converged on {final}, want {want}\nH1={h1}\nH2={h2}\nmerge(H1,H2)={merged_once}\n"
        f"{st.log}\n" + graph_excerpt(c.nodes[1]))


# -- 2. source-priority, then last-write -------------------------------------------------------

@pytest.mark.parametrize("size", SIZES)
def test_source_priority_ranks_listed_nodes_over_unlisted_ones(make_cluster, size):
    """Chain [source-priority nodes=[n2, n0], last-write]. Document P: every node writes, n2
    first (so last-write alone would never pick it) - n2 wins. Q: everyone but n2, n0 first - n0
    wins over every unlisted node. R: only unlisted nodes write - the last of them wins."""
    c = make_cluster(size)
    ids = [node_id(n) for n in c.nodes]
    set_chain(c, [{"kind": "source-priority", "nodes": [ids[2], ids[0]]}, {"kind": "last-write"}])
    P, Q, R = doc_uuid(10), doc_uuid(11), doc_uuid(12)
    for d in (P, Q, R):
        must_write(c.nodes[0], d, {"id": d, "v": "base"})
    settle(c)

    unlisted = [i for i in range(size) if i not in (0, 2)]
    st = Stages(c)
    st.isolate()
    for i in [2, 0] + unlisted:
        must_write(c.nodes[i], P, {"v": f"n{i}"})
        time.sleep(0.02)
    for i in [0] + unlisted:
        must_write(c.nodes[i], Q, {"v": f"n{i}"})
        time.sleep(0.02)
    for i in unlisted:
        must_write(c.nodes[i], R, {"v": f"n{i}"})
        time.sleep(0.02)
    st.pull(1, 0)
    st.pull(1, 2)
    st.pull(0, 1)
    if size == 5:
        st.pull(4, 3)
        st.pull(3, 2)
    st.mesh()

    final = converged_docs(c)
    assert final[P]["v"] == "n2", f"P: {final[P]} (n2 is ranked first)\n{st.log}"
    assert final[Q]["v"] == "n0", f"Q: {final[Q]} (n0 outranks every unlisted node)\n{st.log}"
    assert final[R]["v"] == f"n{unlisted[-1]}", f"R: {final[R]} (unlisted only: last write)\n{st.log}"


# -- 3. validity, then last-write --------------------------------------------------------------

VALIDITY_CHAIN = [{"kind": "validity"}, {"kind": "last-write"}]


def create_schema(n) -> None:
    r = helper("exec", "--addr", n.sql_addr, "--namespace", n.namespace,
               "--sql", "CREATE TABLE t (v INT NOT NULL)", check=False)
    assert r.returncode == 0, f"CREATE TABLE failed: {r.stdout} {r.stderr}"


def is_valid(body: dict) -> bool:
    return isinstance(body.get("v"), int) and not isinstance(body.get("v"), bool)


@pytest.mark.parametrize("size", SIZES)
def test_validity_prefers_the_valid_value_once_every_merger_has_the_schema(make_cluster, size):
    """The schema (v INT NOT NULL) is created on n0 while the others are isolated from it, so
    they still accept invalid values: odd nodes write a string (the later writes), even nodes an
    int. Every merging node pulls the schema before it merges. The valid value must win over a
    later invalid one; among valid values the later wins."""
    c = make_cluster(size)
    set_chain(c, VALIDITY_CHAIN)
    doc = doc_uuid(20)
    must_write(c.nodes[0], doc, {"id": doc, "v": 0})
    settle(c)

    st = Stages(c)
    st.isolate()
    create_schema(c.nodes[0])
    status, _ = write(c.nodes[0], doc, {"v": "rejected"})
    assert status == 400, "n0 has the schema and must refuse an invalid write"
    order = list(range(1, size))
    for i in order:
        value = i * 10 if i % 2 == 0 else f"bad-n{i}"
        must_write(c.nodes[i], doc, {"v": value})
        time.sleep(0.02)
    last_valid = max(i for i in order if i % 2 == 0) * 10
    for i in order:
        st.pull(i, 0)      # the schema first (n0 never wrote the document: no conflict)
    st.pull(1, 2)
    if size == 5:
        st.pull(3, 4)
        st.pull(4, 1)
    st.mesh()

    final = converged_docs(c)[doc]
    assert final["v"] == last_valid, f"got {final}, want v={last_valid}\n{st.log}"


@pytest.mark.parametrize("size", [3])
def test_validity_before_the_schema_arrives(make_cluster, size):
    """The same race with the merge attempted by a node that has not received the schema yet.
    Under a version 2 chain it holds the conflict rather than settling by last-write, and merges by
    validity once the schema reaches it."""
    c = make_cluster(size)
    set_chain(c, VALIDITY_CHAIN)
    doc = doc_uuid(21)
    must_write(c.nodes[0], doc, {"id": doc, "v": 0})
    settle(c)

    st = Stages(c)
    st.isolate()
    create_schema(c.nodes[0])
    must_write(c.nodes[2], doc, {"v": 2})
    time.sleep(0.02)
    must_write(c.nodes[1], doc, {"v": "bad-n1"})   # later, invalid
    st.pull(2, 1)          # n2 has no schema: it must hold, not settle by last-write
    held = read_doc(c.nodes[2], doc)[0]
    assert held == {"id": doc, "v": 2}, f"n2 settled without the schema: {held}\n{st.log}"
    assert conflicts(c.nodes[2]), "n2 should hold the divergence until the schema arrives"
    st.mesh()

    final = converged_docs(c)[doc]
    assert is_valid(final), f"an invalid value survived the merge: {final}\n{st.log}"


# -- 4. procedure ------------------------------------------------------------------------------

# Sums a counter and unions a tag list, against the base. The sum is the textbook three-way
# counter merge (local + remote - base): correct for one base, and wrong whenever the base handed
# in is not the true merge base of the two histories. The union is idempotent, so a wrong base
# cannot hurt it - which is the contrast this scenario draws.
MERGE_PROC = """
function main(c) {
  var num = function (d) { return (d && typeof d.n === "number") ? d.n : 0; };
  var tags = function (d) { return (d && d.tags) ? d.tags : []; };
  var src = c.local || c.remote;
  var out = {};
  for (var k in src) { out[k] = src[k]; }
  if (src && typeof src.n === "number") {
    out.n = num(c.local) + num(c.remote) - num(c.base);
  }
  if (src && src.tags) {
    var seen = {}, all = [];
    var add = function (t) { if (!seen[t]) { seen[t] = true; all.push(t); } };
    tags(c.local).forEach(add);
    tags(c.remote).forEach(add);
    all.sort();
    out.tags = all;
  }
  return { doc: out };
}
"""
PROC_CHAIN_NAME = "sumAndUnion"


def setup_procedure_chain(c: Cluster) -> None:
    set_procedure(c, PROC_CHAIN_NAME, MERGE_PROC)
    set_chain(c, [{"kind": "procedure", "name": PROC_CHAIN_NAME}, {"kind": "last-write"}])


def increment(n, doc: str, by: int = 1) -> dict | None:
    return cas_update(n, doc, lambda b: {"n": b["n"] + by})


def add_tag(n, doc: str, tag: str) -> dict | None:
    return cas_update(n, doc, lambda b: {"tags": sorted(set(b.get("tags") or []) | {tag})})


@pytest.mark.parametrize("size", SIZES)
def test_procedure_union_is_order_independent(make_cluster, size):
    """Each node adds its own tag while isolated; any merge order must end with every tag."""
    c = make_cluster(size)
    setup_procedure_chain(c)
    doc = doc_uuid(30)
    must_write(c.nodes[0], doc, {"id": doc, "tags": []})
    settle(c)

    st = Stages(c)
    st.isolate()
    for i, n in enumerate(c.nodes):
        assert add_tag(n, doc, f"t{i}")
    st.pull(0, 1)
    st.pull(1, 2)
    st.pull(2, 0)
    if size == 5:
        st.pull(3, 0)
        st.pull(4, 1)
    # A second round after the merges, so later merges see several merge bases.
    for i, n in enumerate(c.nodes):
        assert add_tag(n, doc, f"u{i}")
    st.mesh()

    final = converged_docs(c)[doc]
    want = sorted([f"t{i}" for i in range(size)] + [f"u{i}" for i in range(size)])
    assert final["tags"] == want, f"got {final}, want tags {want}\n{st.log}"


def digit_counts(value: int, places: int) -> list[int]:
    """How many times each increment (10**k) is counted in value."""
    return [(value // 10 ** k) % 10 for k in range(places)]


@pytest.mark.parametrize("size", SIZES)
def test_procedure_counter_sum_is_the_total_of_increments(make_cluster, size):
    """Each node increments a counter once while isolated, node i by 10**i, so every increment
    owns one decimal digit and the converged value says how many times each was counted. Chosen
    nodes merge in different orders, two of them increment again (by the next digits), and the
    mesh merges the rest. A counter-summing procedure must count every increment exactly once."""
    c = make_cluster(size)
    setup_procedure_chain(c)
    doc = doc_uuid(31)
    must_write(c.nodes[0], doc, {"id": doc, "n": 0})
    settle(c)

    st = Stages(c)
    st.isolate()
    for i, n in enumerate(c.nodes):
        assert increment(n, doc, 10 ** i)
    st.pull(0, 1)          # n0 = A+B
    st.pull(1, 2)          # n1 = B+C
    st.pull(2, 0)          # n2 = C+(A+B)
    if size == 5:
        st.pull(3, 0)
        st.pull(3, 2)      # n3 = D + (A+B) + (C+(A+B))
        st.pull(4, 2)
        st.pull(4, 1)      # n4 = E + (C+(A+B)) + (B+C)
    mid = {f"n{i}": read_doc(n, doc)[0]["n"] for i, n in enumerate(c.nodes)}
    # Another increment on two merged nodes: their heads now differ, so the merge between them is
    # a real one, over a criss-cross.
    assert increment(c.nodes[0], doc, 10 ** size)
    assert increment(c.nodes[1], doc, 10 ** (size + 1))
    st.mesh()

    places = size + 2
    final = converged_docs(c)[doc]
    counts = digit_counts(final["n"], places)
    assert counts == [1] * places, (
        f"converged on n={final['n']}: increment k (10**k) counted {counts} times, want once each\n"
        f"after the ordered merges: {mid}\n{st.log}\n" + graph_excerpt(c.nodes[0]))


@pytest.mark.parametrize("size", SIZES)
@pytest.mark.xfail(strict=True, reason=(
    "F3, by design: merges are state-based - a document both sides hold with the same value is "
    "not a conflict (peersync/merge_resolution.go resolveDivergedLocked skips sameBody; "
    "peersync/join.go joinSameContent joins equal trees), so the procedure never runs and one "
    "side's increment is lost. A procedure must be a state-based join (union, max, per-node "
    "counter fields); the user guide's counter example is being corrected separately"))
def test_procedure_counter_sum_when_both_sides_reach_the_same_value(make_cluster, size):
    """n0 and n1 each increment the counter by 1 while isolated. Both now hold n=1, from two
    different increments; the summing procedure should make it 2."""
    c = make_cluster(size)
    setup_procedure_chain(c)
    doc = doc_uuid(32)
    must_write(c.nodes[0], doc, {"id": doc, "n": 0})
    settle(c)

    st = Stages(c)
    st.isolate()
    assert increment(c.nodes[0], doc, 1)
    assert increment(c.nodes[1], doc, 1)
    st.pull(0, 1)
    st.mesh()

    final = converged_docs(c)[doc]
    assert final["n"] == 2, f"two increments of 1 converged on n={final['n']}\n{st.log}"


# -- 5. a chain ending in queue ----------------------------------------------------------------

def resolve_until_converged(c: Cluster, take: str = "local", timeout: float = 120.0,
                            max_resolutions: int = 50) -> list[str]:
    """Settles queued conflicts through the control plane, one at a time on whichever node holds
    one, until the cluster converges with none open. Returns what was done."""
    done: list[str] = []
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c.wait_converged(timeout=4)
            return done
        except AssertionError:
            pass
        for i, n in enumerate(c.running()):
            open_ = conflicts(n)
            if not open_:
                continue
            e = open_[0]
            choices = {it["documentId"]: {"take": take} for it in e.get("items") or []}
            status, body = control(n, "POST", ns_path(n, f"/conflicts/{e['id']}/resolve"),
                                   {"choices": choices})
            done.append(f"n{i} resolve {e['id']} ({e.get('kind')}, {len(choices)} docs) -> {status}")
            assert len(done) <= max_resolutions, "resolutions are not converging:\n" + "\n".join(done)
            break
    raise AssertionError("did not converge while resolving\n" + "\n".join(done) + "\n" + c.diagnose())


def queued_by_node(c: Cluster) -> dict[str, int]:
    return {f"n{i}": len(conflicts(n)) for i, n in enumerate(c.running())}


@pytest.mark.parametrize("size", SIZES)
def test_queue_chain_queues_collisions_and_resolves_them(make_cluster, size):
    """Chain [field-merge, queue]. Every node writes the same field x of one document (a
    collision) and its own field of another (disjoint). The disjoint one merges automatically; the
    collision is queued - by every node that tries the merge - until it is settled through the
    control plane, after which the cluster converges with nothing open."""
    c = make_cluster(size)
    set_chain(c, [{"kind": "field-merge"}, {"kind": "queue"}])
    hot, cold = doc_uuid(40), doc_uuid(41)
    must_write(c.nodes[0], hot, {"id": hot, "x": "base"})
    must_write(c.nodes[0], cold, {"id": cold, **{f"f{i}": 0 for i in range(size)}})
    settle(c)

    st = Stages(c)
    st.isolate()
    for i, n in enumerate(c.nodes):
        must_write(n, hot, {"x": f"n{i}"})
        must_write(n, cold, {f"f{i}": i + 1})
    st.mesh()

    # Every node should come to hold a queued conflict for the collision.
    wait_for("every node to queue the collision",
             lambda: all(conflicts(n) for n in c.nodes), timeout=60, diag=c.diagnose)
    queued = queued_by_node(c)
    for n in c.nodes:
        for e in conflicts(n):
            docs_in = {it["documentId"] for it in e.get("items") or []}
            assert docs_in == {hot}, f"only the collision may be queued, got {docs_in}"
    steps = resolve_until_converged(c)

    final = converged_docs(c)
    assert final[hot]["x"] in {f"n{i}" for i in range(size)}, final[hot]
    assert final[cold] == {"id": cold, **{f"f{i}": i + 1 for i in range(size)}}, final[cold]
    print(f"queued per node before resolving: {queued}; resolutions: {len(steps)}")


# -- 6. no chain: the node's --peer-conflict-policy --------------------------------------------

@pytest.mark.parametrize("size", SIZES)
def test_no_chain_last_write_policy_keeps_the_latest_write(make_cluster, size):
    """No chain, --peer-conflict-policy last-write: every node writes the same documents while
    isolated, in a known order. Whatever order the merges run in, each converges on the latest."""
    c = make_cluster(size, conflict_policy="last-write")
    docs = [doc_uuid(50 + k) for k in range(3)]
    for d in docs:
        must_write(c.nodes[0], d, {"id": d, "v": "base"})
    settle(c)

    st = Stages(c)
    st.isolate()
    rng = random.Random(size)
    last = {}
    for d in docs:
        order = list(range(size))
        rng.shuffle(order)
        for i in order:
            must_write(c.nodes[i], d, {"v": f"n{i}"})
            time.sleep(0.01)
        last[d] = f"n{order[-1]}"
    st.pull(0, 1)
    st.pull(2, 0)
    st.mesh()

    final = converged_docs(c)
    for d in docs:
        assert final[d]["v"] == last[d], f"{d}: {final[d]}, want {last[d]}\n{st.log}"


@pytest.mark.parametrize("size", SIZES)
def test_no_chain_strict_policy_queues_and_converges_once_resolved(make_cluster, size):
    """No chain, --peer-conflict-policy strict. Correct behaviour: nothing is settled silently -
    every node that meets the divergence queues it, no node's documents change to another node's
    value on its own - and once the queued conflicts are settled through the control plane, the
    mesh converges with none open."""
    c = make_cluster(size, conflict_policy="strict")
    doc = doc_uuid(60)
    must_write(c.nodes[0], doc, {"id": doc, "v": "base"})
    settle(c)

    st = Stages(c)
    st.isolate()
    for i, n in enumerate(c.nodes):
        must_write(n, doc, {"v": f"n{i}"})
    st.mesh()

    wait_for("every node to queue the divergence", lambda: all(conflicts(n) for n in c.nodes),
             timeout=60, diag=c.diagnose)
    # Strict settles nothing by itself: each node still holds its own write.
    for i, n in enumerate(c.nodes):
        assert read_doc(n, doc)[0]["v"] == f"n{i}", f"n{i} was settled without an operator"
    queued = queued_by_node(c)
    steps = resolve_until_converged(c)
    final = converged_docs(c)
    assert final[doc]["v"] in {f"n{i}" for i in range(size)}, final[doc]
    print(f"queued per node before resolving: {queued}; resolutions: {len(steps)}")


# -- load variants (nightly) -------------------------------------------------------------------

class Loop(threading.Thread):
    """Runs step(i) on one node until stopped; step returns a truthy ack or None (a failure,
    expected while a node is down). Acks are kept in order."""

    def __init__(self, step, limit: int = 0):
        super().__init__(daemon=True)
        self.step, self.limit = step, limit
        self.stop_event = threading.Event()
        self.acks: list = []
        self.failures = 0

    def run(self) -> None:
        i = 0
        while not self.stop_event.is_set() and (not self.limit or i < self.limit):
            try:
                ack = self.step(i)
            except Exception:
                ack = None
            if ack:
                self.acks.append(ack)
                i += 1
            else:
                self.failures += 1
                time.sleep(0.1)

    def stop(self) -> None:
        self.stop_event.set()
        self.join(timeout=60)


def run_load(c: Cluster, loops: list[Loop], seconds: float, kills: int, seed: int) -> None:
    for lp in loops:
        lp.start()
    rng = random.Random(seed)
    end = time.time() + seconds
    for _ in range(kills):
        time.sleep(rng.uniform(1.0, 2.5))
        victim = c.nodes[rng.randrange(len(c.nodes))]
        victim.kill9()
        time.sleep(rng.uniform(0.3, 1.0))
        victim.restart()
    while time.time() < end:
        time.sleep(0.2)
    for lp in loops:
        lp.stop()


def patch_step(n, docs: list[str], field: str):
    def step(i):
        d = docs[i % len(docs)]
        status, body = write(n, d, {field: i})
        return (d, i) if status == 200 else None
    return step


def fetch_graph(n) -> dict[str, list[str]]:
    """The whole commit graph reachable from n's head, as {hash: parents}."""
    out: dict[str, list[str]] = {}
    skip = 0
    while True:
        status, body = control(n, "GET", ns_path(n, f"/log?limit=500&skip={skip}"), timeout=60)
        assert status == 200, body
        for cm in body.get("commits") or []:
            out[cm["hash"]] = cm.get("parents") or []
        if not body.get("hasMore"):
            return out
        skip = body["nextSkip"]


def descends(graph: dict[str, list[str]], commit: str, ancestor: str) -> bool:
    seen, stack = set(), [commit]
    while stack:
        h = stack.pop()
        if h == ancestor:
            return True
        if h in seen:
            continue
        seen.add(h)
        stack.extend(graph.get(h, []))
    return False


@pytest.mark.slow
@pytest.mark.parametrize("kills", [0, pytest.param(2, marks=pytest.mark.destructive)])
@pytest.mark.parametrize("size", SIZES)
def test_load_field_merge_keeps_each_nodes_own_field(make_cluster, size, kills):
    """Every node patches its own field of the same documents as fast as it can (with kills: one
    node is killed and restarted twice). No two nodes ever write the same field, so field-merge
    alone must settle every merge: each field ends at its node's last acknowledged value."""
    c = make_cluster(size)
    set_chain(c, FIELD_MERGE_CHAIN)
    docs = [doc_uuid(100 + k) for k in range(4)]
    for d in docs:
        must_write(c.nodes[0], d, {"id": d, **{f"f{i}": -1 for i in range(size)}})
    settle(c)
    loops = [Loop(patch_step(n, docs, f"f{i}")) for i, n in enumerate(c.nodes)]
    run_load(c, loops, seconds=12, kills=kills, seed=size)

    final = converged_docs(c, timeout=180)
    wrong = []
    for i, lp in enumerate(loops):
        last: dict[str, int] = {}
        for d, v in lp.acks:
            last[d] = v
        for d, v in last.items():
            if final[d].get(f"f{i}") != v:
                wrong.append((d[-6:], f"f{i}", final[d].get(f"f{i}"), v))
    assert not wrong, (f"{len(wrong)} fields not at their node's last acknowledged value "
                       f"(doc, field, got, want): {wrong[:10]}\n" + graph_excerpt(c.nodes[0]))


@pytest.mark.slow
@pytest.mark.destructive
@pytest.mark.parametrize("size", SIZES)
def test_load_source_priority_top_node_wins(make_cluster, size):
    """Every node writes the same documents; chain [source-priority [n2, n0], last-write]. Under
    load another node may legitimately overwrite n2's value after seeing it - source-priority
    settles concurrent writes, not later ones - so the check is causal: a document ends at n2's
    last acknowledged write to it unless some other node's write descends from that write."""
    c = make_cluster(size)
    ids = [node_id(n) for n in c.nodes]
    set_chain(c, [{"kind": "source-priority", "nodes": [ids[2], ids[0]]}, {"kind": "last-write"}])
    docs = [doc_uuid(110 + k) for k in range(4)]
    for d in docs:
        must_write(c.nodes[0], d, {"id": d, "v": "base"})
    settle(c)

    def step_for(i, n):
        def step(k):
            d = docs[k % len(docs)]
            status, body = write(n, d, {"v": f"n{i}-{k}"})
            return (d, f"n{i}-{k}", body["commit"]) if status == 200 else None
        return step

    loops = [Loop(step_for(i, n)) for i, n in enumerate(c.nodes)]
    run_load(c, loops, seconds=12, kills=2, seed=100 + size)
    final = converged_docs(c, timeout=180)
    graph = fetch_graph(c.nodes[0])
    last = {}
    for d, v, cm in loops[2].acks:
        last[d] = (v, cm)
    wrong = []
    for d, (v, cm) in last.items():
        if final[d]["v"] == v:
            continue
        overwritten = [w for i, lp in enumerate(loops) if i != 2 for dd, w, wc in lp.acks
                       if dd == d and descends(graph, wc, cm)]
        if not overwritten:
            wrong.append((d[-6:], final[d]["v"], v))
    assert not wrong, (f"n2's last write lost without being overwritten causally "
                       f"(doc, got, n2's last): {wrong}")


@pytest.mark.slow
@pytest.mark.destructive
@pytest.mark.parametrize("size", SIZES)
@pytest.mark.xfail(strict=False, reason=(
    "F3 (see test_procedure_counter_sum_when_both_sides_reach_the_same_value): under load two "
    "nodes often reach the same count from different increments, which a state-based merge never "
    "offers the procedure, so the total falls short"))
def test_load_procedure_counter_sum(make_cluster, size):
    """Every node increments the same counters under compare-and-set while one node is killed;
    the summing procedure must converge on exactly the number of acknowledged increments."""
    c = make_cluster(size)
    setup_procedure_chain(c)
    docs = [doc_uuid(120 + k) for k in range(2)]
    for d in docs:
        must_write(c.nodes[0], d, {"id": d, "n": 0})
    settle(c)

    def step_for(n):
        def step(k):
            d = docs[k % len(docs)]
            return d if increment(n, d, 1) else None
        return step

    loops = [Loop(step_for(n)) for n in c.nodes]
    run_load(c, loops, seconds=12, kills=1, seed=200 + size)
    final = converged_docs(c, timeout=180)
    acked = {d: sum(1 for lp in loops for x in lp.acks if x == d) for d in docs}
    got = {d: final[d]["n"] for d in docs}
    assert got == acked, f"counter totals {got}, acknowledged increments {acked}"


@pytest.mark.slow
@pytest.mark.destructive
@pytest.mark.parametrize("size", SIZES)
def test_load_no_chain_last_write_converges_on_the_latest_write(make_cluster, size):
    """No chain, last-write policy, every node writing the same documents with one killed: each
    document converges on a value some node wrote, written no earlier (to the millisecond) than
    any other acknowledged write of that document."""
    c = make_cluster(size, conflict_policy="last-write")
    docs = [doc_uuid(130 + k) for k in range(4)]
    for d in docs:
        must_write(c.nodes[0], d, {"id": d, "v": "base"})
    settle(c)

    def step_for(i, n):
        def step(k):
            d = docs[k % len(docs)]
            status, body = write(n, d, {"v": f"n{i}-{k}"})
            return (d, f"n{i}-{k}", body["commit"]) if status == 200 else None
        return step

    loops = [Loop(step_for(i, n)) for i, n in enumerate(c.nodes)]
    run_load(c, loops, seconds=10, kills=1, seed=300 + size)
    final = converged_docs(c, timeout=180)
    by_value = {v: (d, cm) for lp in loops for d, v, cm in lp.acks}
    probe = c.nodes[0]
    for d in docs:
        v = final[d]["v"]
        assert v in by_value, f"{d} holds {v}, which no node acknowledged"
        won = commit_ms(probe, by_value[v][1])
        latest = max(commit_ms(probe, cm) for dd, _, cm in (a for lp in loops for a in lp.acks)
                     if dd == d)
        assert won >= latest, f"{d}: winner {v} at {won} ms, but a write at {latest} ms exists"


@pytest.mark.slow
@pytest.mark.destructive
@pytest.mark.parametrize("policy_chain", ["strict", "queue"])
@pytest.mark.parametrize("size", SIZES)
def test_load_strict_and_queue_resolve_then_converge(make_cluster, size, policy_chain):
    """Every node writes the same documents with one killed; nothing settles a collision
    automatically (strict policy, or a [field-merge, queue] chain). Settling the queue through the
    control plane must bring the mesh to one head with nothing open."""
    c = make_cluster(size, conflict_policy="strict")
    if policy_chain == "queue":
        set_chain(c, [{"kind": "field-merge"}, {"kind": "queue"}])
    docs = [doc_uuid(140 + k) for k in range(3)]
    for d in docs:
        must_write(c.nodes[0], d, {"id": d, "v": "base"})
    settle(c)

    def step_for(i, n):
        def step(k):
            status, _ = write(n, docs[k % len(docs)], {"v": f"n{i}-{k}"})
            return status == 200
        return step

    loops = [Loop(step_for(i, n), limit=30) for i, n in enumerate(c.nodes)]
    run_load(c, loops, seconds=8, kills=1, seed=400 + size)
    resolve_until_converged(c, timeout=240, max_resolutions=200)
    final = converged_docs(c)
    written = {f"n{i}-{k}" for i in range(size) for k in range(30)} | {"base"}
    for d in docs:
        assert final[d]["v"] in written, final[d]
