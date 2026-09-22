"""Resolver authority between real processes (docs/kdb-distributed-self-healing-research.md,
Phase 10.5).

Node A replicates with B through --peer and tells a resolver authority - here a small HTTP
receiver standing in for the application service - about the conflicts its namespace's chain
hands it, via --conflict-webhook. The chain is set on A's control plane and replicates to B through
_kdb/meta; the authority settles the conflict on A's control plane and the settlement replicates.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from server_fixtures import KdbServer, free_port, get, put

pytestmark = [pytest.mark.server, pytest.mark.cluster]

DOC = "00000000000000000000000000000c0f"
SECRET = "e2e-webhook-secret"


def wait_for(what: str, cond, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


class Authority:
    """A webhook endpoint that checks each delivery's signature and records it."""

    def __init__(self) -> None:
        self.received: list[dict] = []
        self.bad_signatures = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                want = "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(self.headers.get("X-KDB-Signature", ""), want):
                    outer.bad_signatures += 1
                    self.send_response(401)
                    self.end_headers()
                    return
                note = json.loads(body)
                assert self.headers.get("X-KDB-Delivery") == note["conflict"]["id"]
                outer.received.append(note)
                self.send_response(204)
                self.end_headers()

            def log_message(self, *args):  # keep pytest output clean
                pass

        self.server = HTTPServer(("127.0.0.1", free_port()), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}/conflicts"

    def close(self) -> None:
        self.server.shutdown()


def control(srv: KdbServer, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    ns = urllib.parse.quote(srv.namespace, safe="")
    req = urllib.request.Request(
        f"http://127.0.0.1:{srv.control_port}/v1/ns/{ns}{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Bearer e2e:e2e", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, json.loads(raw) if raw else {}


def with_control(extra: list[str]) -> KdbServer:
    port = free_port()
    srv = KdbServer(extra_args=["--control-addr", f"127.0.0.1:{port}", "--control-write", *extra])
    srv.control_port = port
    return srv


@pytest.fixture
def authority_pair():
    authority = Authority()
    b = with_control([]).start()
    a = with_control([
        "--peer", f"name=b,addr={b.peer_addr},interval=1500ms",
        "--conflict-webhook", authority.url,
        "--conflict-webhook-secret", SECRET,
        "--conflict-webhook-interval", "300ms",
    ]).start()
    yield a, b, authority
    authority.close()
    for srv in (a, b):
        try:
            code = srv.stop()
            assert code == 0, f"exit {code}:\n{srv.logs()}"
        finally:
            srv.cleanup()


def conflicts(srv: KdbServer, query: str = "") -> list[dict]:
    status, body = control(srv, "GET", "/conflicts" + query)
    assert status == 200, body
    return body["conflicts"]


def test_authority_is_notified_and_its_resolution_replicates(authority_pair):
    a, b, authority = authority_pair
    status, body = control(a, "GET", "/resolution")
    assert status == 200, body
    node_a = body["thisNode"]

    # The chain: hold anything undecided for the authority, and only A notifies it.
    status, body = control(a, "PUT", "/resolution",
                           {"rules": [{"kind": "authority", "pending": "hold", "node": node_a}]})
    assert status == 200, body
    chain_hash = body["hash"]
    wait_for("the chain to replicate to B",
             lambda: control(b, "GET", "/resolution")[1].get("hash") == chain_hash)

    put(a, DOC, {"id": DOC, "v": "base"})
    wait_for("the base on B", lambda: (get(b, DOC) or {}).get("v") == "base")

    # Both write before either sees the other.
    put(b, DOC, {"id": DOC, "v": "from B"})
    put(a, DOC, {"id": DOC, "v": "from A"})

    wait_for("A to hold the conflict for the authority", lambda: conflicts(a, "?authority=true"))
    held = conflicts(a, "?authority=true")[0]
    assert held["authorityNode"] == node_a
    assert json.loads(held["details"][0]["base"])["v"] == "base"
    node_b = control(b, "GET", "/resolution")[1]["thisNode"]
    assert held["details"][0]["incomingOrigin"]["nodeId"] == node_b
    assert held["details"][0]["localOrigin"]["nodeId"] == node_a
    # Hold means no merge on either node meanwhile.
    assert get(a, DOC)["v"] == "from A"
    assert get(b, DOC)["v"] == "from B"

    wait_for("the authority to be notified", lambda: authority.received)
    note = authority.received[0]
    assert note["type"] == "kdb.conflict" and note["node"] == node_a
    assert note["conflict"]["id"] == held["id"]
    assert authority.bad_signatures == 0
    wait_for("the delivery to be recorded", lambda: not conflicts(a, "?authority=true&undelivered=true"))
    # B holds its own copy but is not the notifying node: nothing came from it.
    assert all(n["node"] == node_a for n in authority.received)

    # The authority decides: B's value wins.
    doc_id = held["items"][0]["documentId"]
    status, body = control(a, "POST", f"/conflicts/{held['id']}/resolve",
                           {"choices": {doc_id: {"take": "remote"}}})
    assert status == 200, body

    wait_for("B to converge on the authority's choice",
             lambda: (get(b, DOC) or {}).get("v") == "from B" and (get(a, DOC) or {}).get("v") == "from B")
    wait_for("both queues to clear",
             lambda: not conflicts(a, "?authority=true") and not conflicts(b, "?authority=true"))


def test_provisional_decision_merges_now_and_is_overruled_later(authority_pair):
    a, b, authority = authority_pair
    status, body = control(a, "PUT", "/resolution",
                           {"rules": [{"kind": "authority", "pending": "provisional"}]})
    assert status == 200, body
    chain_hash = body["hash"]
    wait_for("the chain to replicate to B",
             lambda: control(b, "GET", "/resolution")[1].get("hash") == chain_hash)

    put(a, DOC, {"id": DOC, "v": "base"})
    wait_for("the base on B", lambda: (get(b, DOC) or {}).get("v") == "base")
    put(a, DOC, {"id": DOC, "v": "from A"})
    put(b, DOC, {"id": DOC, "v": "from B"})

    # Provisional: both converge at once on one value, and the authority hears about it.
    wait_for("a provisional merge on both nodes",
             lambda: (get(a, DOC) or {}).get("v") == (get(b, DOC) or {}).get("v")
             and conflicts(a, "?authority=true"))
    entry = conflicts(a, "?authority=true")[0]
    assert entry["kind"] == "provisional"
    wait_for("the authority to be notified",
             lambda: any(n["conflict"]["id"] == entry["id"] for n in authority.received))

    kept = json.loads(entry["items"][0]["localDoc"])["v"]
    other = "from A" if kept == "from B" else "from B"
    doc_id = entry["items"][0]["documentId"]
    status, body = control(a, "POST", f"/conflicts/{entry['id']}/resolve",
                           {"choices": {doc_id: {"take": "remote"}}})
    assert status == 200, body
    wait_for("the overruling to reach B",
             lambda: (get(b, DOC) or {}).get("v") == other and (get(a, DOC) or {}).get("v") == other)
    wait_for("the entry to close on both nodes",
             lambda: not conflicts(a, "?authority=true") and not conflicts(b, "?authority=true"))
