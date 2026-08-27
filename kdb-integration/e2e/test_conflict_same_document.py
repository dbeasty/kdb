"""E2e scenario 3 (Phase 3.2): concurrent writes to the SAME document on two peers.

Component 39's conflict path - only disjoint-document auto-merge had integration coverage
before this. Under the strict default, a same-document divergence must surface as a conflict
(never silently resolve); under LAST_WRITE, every node must converge on the same winner (the
later timestamp) regardless of sync direction - the 1-G10/1-G11 determinism fixes.
"""

from __future__ import annotations

import time

import pytest

from server_fixtures import get, helper, put, relay

pytestmark = [pytest.mark.server, pytest.mark.cluster]

DOC = "00000000000000000000000000000d0c"


def test_strict_policy_reports_conflict_not_silent_resolution(cluster):
    a, b = cluster(2)

    put(a, DOC, {"id": DOC, "winner": "a"})
    time.sleep(0.05)
    put(b, DOC, {"id": DOC, "winner": "b"})

    # The relay pulls A's history, then hits B where the same document diverged: with the
    # strict default this must fail loudly with a conflict, not silently pick a side.
    r = helper("relay", "--namespace", a.namespace,
               "--servers", f"{a.peer_addr},{b.peer_addr}", "--rounds", "1", check=False)
    assert r.returncode != 0, "strict policy silently resolved a same-document divergence"
    assert any(s in (r.stdout + r.stderr).lower() for s in ("conflict", "divergent"))

    # And neither server had its head hijacked: each still serves its own write.
    assert get(a, DOC)["winner"] == "a"
    assert get(b, DOC)["winner"] == "b"


def test_last_write_converges_on_the_later_write_everywhere(cluster):
    a, b = cluster(2, extra_args=["--peer-conflict-policy", "last-write"])

    put(a, DOC, {"id": DOC, "winner": "a"})
    time.sleep(0.05)
    put(b, DOC, {"id": DOC, "winner": "b"})  # later timestamp - must win everywhere

    relay_lastwrite([a, b])
    winner_a = get(a, DOC)["winner"]
    winner_b = get(b, DOC)["winner"]
    assert winner_a == winner_b == "b", (
        f"peers disagree or later write lost: a={winner_a} b={winner_b}")


def test_last_write_same_winner_regardless_of_sync_direction(cluster):
    # The same divergence relayed in the opposite order must land on the identical winner (the
    # old "incoming side always wins" bug picked opposite winners per direction).
    a, b = cluster(2, extra_args=["--peer-conflict-policy", "last-write"])

    put(a, DOC, {"id": DOC, "winner": "a"})
    time.sleep(0.05)
    put(b, DOC, {"id": DOC, "winner": "b"})

    relay_lastwrite([b, a])  # reversed direction vs. the test above
    winner_a = get(a, DOC)["winner"]
    winner_b = get(b, DOC)["winner"]
    assert winner_a == winner_b == "b", (
        f"direction-dependent winner: a={winner_a} b={winner_b}")


def relay_lastwrite(servers, rounds: int = 2):
    uris = ",".join(s.peer_addr for s in servers)
    return helper("relay", "--namespace", servers[0].namespace, "--servers", uris,
                  "--rounds", str(rounds), "--conflict-policy", "last-write")
