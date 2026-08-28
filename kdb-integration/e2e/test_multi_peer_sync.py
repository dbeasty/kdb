"""E2e scenario 2 (Phase 3.2): multi-peer (3-node) sync with transitive propagation.

Three independent kdb-service processes; peer-sync sessions between them are carried by the
kdb-e2e-helper's full-peer relay (servers listen for peer sync; sessions are client-initiated
by design). A write at A must reach C - which never talks to A directly here - via the relay's
local replica passing through B's round.
"""

from __future__ import annotations

import pytest

from server_fixtures import KdbServer, get, put, relay

pytestmark = [pytest.mark.server, pytest.mark.cluster]

DOC_A = "000000000000000000000000000000a1"
DOC_B = "000000000000000000000000000000b2"
DOC_C = "000000000000000000000000000000c3"


def test_write_at_a_propagates_transitively_to_c(cluster):
    a, b, c = cluster(3)

    put(a, DOC_A, {"id": DOC_A, "origin": "a"})

    # One relay pass A -> B -> C: the relay peer pulls A's history, then pushes it onward.
    relay([a, b, c], rounds=1)

    assert get(c, DOC_A)["origin"] == "a", f"A's write did not reach C:\n{c.logs()}"
    assert get(b, DOC_A)["origin"] == "a"


def test_disjoint_writes_converge_everywhere(cluster):
    a, b, c = cluster(3)

    put(a, DOC_A, {"id": DOC_A, "origin": "a"})
    put(b, DOC_B, {"id": DOC_B, "origin": "b"})
    put(c, DOC_C, {"id": DOC_C, "origin": "c"})

    # Two rounds: round 1 carries A's history into B/C (and picks up theirs on the way);
    # round 2 carries what was learned late back to the earlier servers.
    relay([a, b, c], rounds=2)

    for srv in (a, b, c):
        for doc, origin in ((DOC_A, "a"), (DOC_B, "b"), (DOC_C, "c")):
            body = get(srv, doc)
            assert body is not None and body["origin"] == origin, (
                f"{origin}'s write missing on a node:\n{srv.logs()}")


def test_node_joining_late_catches_up(cluster):
    a, b = cluster(2)
    put(a, DOC_A, {"id": DOC_A, "version": 1})
    relay([a, b], rounds=1)
    assert get(b, DOC_A)["version"] == 1

    # A third node joins after the fact and catches up from B alone.
    late = KdbServer().start()
    try:
        relay([b, late], rounds=1)
        assert get(late, DOC_A)["version"] == 1
    finally:
        code = late.stop()
        late.cleanup()
        assert code == 0
