"""Multi-node cluster harness for the e2e suite.

`Cluster` starts N real kdb-service processes wired to each other with --peer (the replicator,
not the relay helper), each on ports pinned across restarts and with the control plane on, so a
test can crash, stop and restart nodes and still read every node's head, documents, conflict
queue and per-peer progress.

The convergence check is deliberately strict. Every node must report the same head commit hash,
twice in a row, with the same documents and an empty conflict queue. Values are not enough on
their own: two nodes can hold the same documents on different histories and diverge on the next
write.
"""

from __future__ import annotations

import base64
import json
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

from server_fixtures import KdbServer, helper, put

CONTROL_AUTH = "Basic " + base64.b64encode(b"e2e:e2e").decode()


# -- control plane ---------------------------------------------------------------------------

def control(srv: KdbServer, method: str, path: str, body: dict | None = None,
            timeout: float = 10.0) -> tuple[int, dict]:
    url = f"http://127.0.0.1:{srv.control_port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", CONTROL_AUTH)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def ns_path(srv: KdbServer, suffix: str = "", namespace: str | None = None) -> str:
    return f"/v1/ns/{urllib.parse.quote(namespace or srv.namespace, safe='')}{suffix}"


def head(srv: KdbServer, namespace: str | None = None) -> str | None:
    status, body = control(srv, "GET", ns_path(srv, "/status", namespace))
    return body.get("head") if status == 200 else None


def documents(srv: KdbServer, namespace: str | None = None) -> dict[str, str]:
    """Every document at head, as {docId: preview}, following the listing's cursor."""
    out: dict[str, str] = {}
    cursor = ""
    while True:
        q = "?limit=500" + (f"&cursor={urllib.parse.quote(cursor)}" if cursor else "")
        status, body = control(srv, "GET", ns_path(srv, "/docs", namespace) + q)
        if status != 200:
            raise AssertionError(f"docs listing on {srv.sql_addr} failed ({status}): {body}")
        for d in body.get("documents") or []:
            out[d["docId"]] = d.get("preview", "")
        if not body.get("hasMore"):
            return out
        cursor = body["nextCursor"]


def conflicts(srv: KdbServer, namespace: str | None = None) -> list[dict]:
    status, body = control(srv, "GET", ns_path(srv, "/conflicts", namespace))
    if status != 200:
        raise AssertionError(f"conflict listing on {srv.sql_addr} failed ({status}): {body}")
    return body.get("conflicts") or []


def peers(srv: KdbServer) -> dict:
    return control(srv, "GET", "/v1/peers")[1]


def uuid_of(doc_id: str) -> str:
    """The control plane names documents as UUIDs; the wire client as 32 hex characters."""
    h = doc_id.replace("-", "").lower()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


# -- cluster ---------------------------------------------------------------------------------

@dataclass
class Cluster:
    """N kdb-service nodes replicating with --peer.

    topology:
      mesh  - every node peers with every other (each pair is synced from both ends)
      chain - node i peers with node i+1 only; data must hop through every node in between
      star  - every node peers with node 0, the hub
    """

    size: int
    topology: str = "mesh"
    interval: str = "300ms"
    conflict_policy: str = "strict"
    node_args: list = field(default_factory=list)
    control_write: bool = False
    # service_bins: the kdb-service binary per node (index i is node i; "" or a missing entry
    # means today's binary). Mixed-version clusters start this way; Cluster.switch_binary moves
    # one node to another binary on the same data dir and ports.
    service_bins: list = field(default_factory=list)
    nodes: list[KdbServer] = field(default_factory=list)

    def __post_init__(self) -> None:
        bins = list(self.service_bins) + [""] * (self.size - len(self.service_bins))
        self.nodes = [
            KdbServer(control=True, control_write=self.control_write, pin_ports=True,
                      service_bin=bins[i]).reserve_ports()
            for i in range(self.size)
        ]
        for i, node in enumerate(self.nodes):
            node.extra_args = list(self.node_args) + [
                "--peer-conflict-policy", self.conflict_policy]
            for j in self.peers_of(i):
                node.extra_args += [
                    "--peer", f"name=n{j},addr={self.nodes[j].peer_addr},interval={self.interval}"]

    def peers_of(self, i: int) -> list[int]:
        if self.topology == "mesh":
            return [j for j in range(self.size) if j != i]
        if self.topology == "chain":
            return [i + 1] if i + 1 < self.size else []
        if self.topology == "star":
            return [0] if i != 0 else []
        raise ValueError(f"unknown topology {self.topology}")

    def start(self) -> "Cluster":
        for node in self.nodes:
            node.start()
        return self

    def switch_binary(self, i: int, binary: str, kill: bool = False) -> KdbServer:
        """Moves node i to `binary`: a clean stop (asserted to exit 0), or SIGKILL when `kill`,
        then a restart on the same data dir and the same pinned ports - one step of a rolling
        upgrade."""
        node = self.nodes[i]
        if kill:
            node.kill9()
        else:
            code = node.stop()
            assert code == 0, f"n{i} did not stop cleanly (exit {code}):\n{tail(node.logs())}"
        return node.restart(service_bin=binary)

    def add_node(self, peer_spec_extra: str = "", peer_of: int = 0,
                 service_bin: str = "") -> KdbServer:
        """A node joining later, peering with one existing node."""
        node = KdbServer(control=True, control_write=self.control_write, pin_ports=True,
                         service_bin=service_bin).reserve_ports()
        spec = f"name=n{peer_of},addr={self.nodes[peer_of].peer_addr},interval={self.interval}"
        if peer_spec_extra:
            spec += "," + peer_spec_extra
        node.extra_args = list(self.node_args) + [
            "--peer-conflict-policy", self.conflict_policy, "--peer", spec]
        self.nodes.append(node)
        return node.start()

    def running(self) -> list[KdbServer]:
        return [n for n in self.nodes if n.proc is not None and n.proc.poll() is None]

    def shutdown(self) -> list[str]:
        """SIGTERM every node; returns a description of each unclean exit."""
        problems = []
        for i, node in enumerate(self.nodes):
            try:
                if node.proc is not None and node.proc.poll() is None:
                    code = node.stop()
                    if code != 0:
                        problems.append(f"n{i} exited {code}:\n{tail(node.logs())}")
            finally:
                node.cleanup()
        return problems

    # -- convergence -------------------------------------------------------------------------

    def wait_converged(self, timeout: float = 90.0, namespace: str | None = None,
                       allow_conflicts: bool = False) -> str:
        """Waits until every running node holds the same head, twice in a row, with identical
        documents and (unless allow_conflicts) no open conflicts. Returns that head."""
        deadline = time.time() + timeout
        last = None
        nodes = self.running()
        while time.time() < deadline:
            heads = [head(n, namespace) for n in nodes]
            if None not in heads and len(set(heads)) == 1:
                if last == heads[0]:
                    docs = [documents(n, namespace) for n in nodes]
                    if all(d == docs[0] for d in docs):
                        if allow_conflicts or not any(conflicts(n, namespace) for n in nodes):
                            return heads[0]
                last = heads[0]
            else:
                last = None
            time.sleep(0.5)
        raise AssertionError("cluster did not converge in %.0fs\n%s" % (timeout, self.diagnose(namespace)))

    def diagnose(self, namespace: str | None = None) -> str:
        lines = []
        for i, n in enumerate(self.nodes):
            alive = n.proc is not None and n.proc.poll() is None
            lines.append(f"== n{i} {'up' if alive else 'DOWN'} sql={n.sql_addr} "
                         f"bin={n.binary}")
            if not alive:
                continue
            try:
                lines.append(f"   head={head(n, namespace)} docs={len(documents(n, namespace))} "
                             f"conflicts={len(conflicts(n, namespace))}")
                for p in peers(n).get("peers") or []:
                    st = p.get("State") or {}
                    ns = (st.get("namespaces") or {}).get(namespace or n.namespace, {})
                    lines.append(
                        f"   peer {p.get('Name')}: failures={st.get('consecutiveFailures')} "
                        f"local={str(ns.get('localMain'))[:12]} remote={str(ns.get('remoteMain'))[:12]} "
                        f"pulled={ns.get('pulled')} pushed={ns.get('pushed')} "
                        f"openConflicts={ns.get('openConflicts')} lastError={st.get('lastError')}")
            except Exception as e:  # diagnostics must never mask the real failure
                lines.append(f"   (control plane unreadable: {e})")
            lines.append(tail(n.all_logs(), levels=("WARN", "ERROR")))
        return "\n".join(lines)


def tail(log: str, lines: int = 25, levels: tuple[str, ...] = ()) -> str:
    rows = log.splitlines()
    if levels:
        rows = [r for r in rows if any(f'"level":"{lv}"' in r for lv in levels) or "panic" in r]
    return "\n".join("   | " + r[:400] for r in rows[-lines:])


# -- load ------------------------------------------------------------------------------------

class Writer(threading.Thread):
    """Writes {"id", "v", "by"} to `docs` round-robin on one node until stopped.

    A write is acknowledged only when the helper printed a commit hash. Failures are expected
    while a node is down and are retried after a pause; `acked` keeps the highest value
    acknowledged per document, which a single-writer-per-document test can hold every node to.
    """

    def __init__(self, node: KdbServer, docs: list[str], tag: str, limit: int = 0,
                 value_base: int = 0) -> None:
        super().__init__(daemon=True)
        self.node, self.docs, self.tag, self.limit = node, docs, tag, limit
        self.value_base = value_base
        self.stop_event = threading.Event()
        self.acked: dict[str, int] = {}
        self.all_acked: list[tuple[str, int]] = []
        self.failures = 0
        self.lock = threading.Lock()

    def run(self) -> None:
        i = 0
        while not self.stop_event.is_set() and (not self.limit or i < self.limit):
            doc = self.docs[i % len(self.docs)]
            v = self.value_base + i
            try:
                r = helper("put", "--addr", self.node.sql_addr, "--namespace", self.node.namespace,
                           "--doc-id", doc, "--json", json.dumps({"id": doc, "v": v, "by": self.tag}),
                           check=False, timeout=15)
                ok = r.returncode == 0 and len(r.stdout.strip()) == 64
            except subprocess.TimeoutExpired:
                ok = False
            if ok:
                with self.lock:
                    self.acked[doc] = v
                    self.all_acked.append((doc, v))
                i += 1
            else:
                self.failures += 1
                time.sleep(0.2)

    def stop(self) -> None:
        self.stop_event.set()
        self.join(timeout=60)

    def snapshot(self) -> dict[str, int]:
        with self.lock:
            return dict(self.acked)


def owned_docs(owner: int, count: int) -> list[str]:
    """Document ids owned by one writer: disjoint across owners, so each has a single writer."""
    return [f"{owner + 1:04x}{i:028x}" for i in range(count)]


def assert_values_at_least(srv: KdbServer, acked: dict[str, int], where: str) -> None:
    """Every acknowledged write must be readable on `srv` at its acknowledged value or later."""
    docs = documents(srv)
    missing, behind = [], []
    for doc, v in acked.items():
        preview = docs.get(uuid_of(doc))
        if preview is None:
            missing.append(doc)
            continue
        got = json.loads(preview).get("v")
        if got is None or got < v:
            behind.append((doc, got, v))
    assert not missing and not behind, (
        f"{where}: {len(missing)} acknowledged documents missing, {len(behind)} behind their "
        f"acknowledged value (of {len(acked)} checked; node holds {len(docs)}).\n"
        f"missing (first 5): {missing[:5]}\nbehind (doc, got, acked) (first 5): {behind[:5]}")


# -- a snapshot hub that is only ever pushed to ----------------------------------------------

def strip_peers(args: list) -> list:
    """`args` without any --peer and its value."""
    out, i = [], 0
    while i < len(args):
        if args[i] == "--peer":
            i += 2
            continue
        out.append(args[i])
        i += 1
    return out


def wait_until(cond, timeout: float, what: str, c: Cluster) -> None:
    deadline = time.time() + timeout
    while not cond():
        if time.time() >= deadline:
            raise AssertionError(f"{what} in {timeout:.0f}s\n{c.diagnose()}")
        time.sleep(0.2)


def peer_state(srv: KdbServer, name: str) -> dict:
    """The replicator's state for srv's peer `name` (/v1/peers)."""
    for p in peers(srv).get("peers") or []:
        if p.get("Name") == name:
            return p.get("State") or {}
    raise AssertionError(f"no peer {name} on {srv.sql_addr}")


def fork_behind_a_pushed_only_hub(c: Cluster) -> tuple[KdbServer, list, dict[str, int]]:
    """Builds, on a two-node mesh, the push-direction twin of #111's late joiner: a hub
    bootstrapped from n0's snapshot, with no --peer of its own, and n1 about to push it a branch
    forked behind the hub's root. Deterministic, by stops rather than timing:

      - from a common head H, n1 writes C while n0 is down, and n0 writes X while n1 is down;
      - the hub is bootstrapped from n0 at X (one sync only), then restarted with no --peer;
      - n1 takes X from n0 (merging it with C), and only then is wired to push to the hub, so
        its first push names X as held and leaves out everything below it, H included - C's
        parent, which the hub does not hold. (A pusher that does not hold the hub's head sends
        its whole history instead, which needs no fork point.)

    Returns the hub, the --peer arguments that push to it (n1 already has them), and the two
    fork documents with their values."""
    n0, n1 = c.nodes[0], c.nodes[1]
    c.wait_converged()
    fork_c, fork_x = owned_docs(6, 1)[0], owned_docs(7, 1)[0]
    assert n0.stop() == 0
    put(n1, fork_c, {"id": fork_c, "v": 1, "by": "c"})
    assert n1.stop() == 0
    n0.restart()
    put(n0, fork_x, {"id": fork_x, "v": 1, "by": "x"})

    # One sync only (the later interval wins): the bootstrap, at X.
    hub = c.add_node("bootstrap=snapshot,mode=pull,interval=1h", peer_of=0)
    wait_until(lambda: head(hub) == head(n0), 30, "the hub never bootstrapped", c)
    assert hub.stop() == 0
    hub.extra_args = strip_peers(hub.extra_args)
    hub.restart()

    n1.restart()
    wait_until(lambda: head(n1) == head(n0), 30, "n0 and n1 did not converge", c)
    assert n1.stop() == 0
    to_hub = ["--peer", f"name=hub,addr={hub.peer_addr},interval={c.interval},mode=push"]
    n1.extra_args += to_hub
    n1.restart()
    return hub, to_hub, {fork_c: 1, fork_x: 1}
