"""E2e scenario 9 (Phase 3.2): restart with a large commit history.

The layer-13 README flags replay-after-restart resource use as a known untested hazard. Build
a history of a couple thousand commits via sustained wire load, restart, and require the
server to become ready again within a hard bound - and still serve reads correctly.
"""

from __future__ import annotations

import time

import pytest

from server_fixtures import KdbServer, get, helper

pytestmark = [pytest.mark.server, pytest.mark.slow]

COMMITS = 2000


def test_restart_after_large_history_is_bounded():
    srv = KdbServer().start()
    try:
        r = helper("load", "--addr", srv.sql_addr, "--namespace", srv.namespace,
                   "--rounds", str(COMMITS), timeout=600)
        assert f"loaded={COMMITS}" in r.stdout

        code = srv.stop()
        assert code == 0

        started = time.time()
        srv.restart()  # wait_ready() polls /readyz with its own 20s default timeout
        startup = time.time() - started
        # Generous but real bound: replaying ~2k commits must not take minutes.
        assert startup < 20, f"restart with {COMMITS}-commit history took {startup:.1f}s"

        # The last write per doc id (load cycles doc ids mod 128) is intact.
        last_doc = f"{(COMMITS - 1) % 128 + 1:032x}"
        body = get(srv, last_doc)
        assert body is not None and body["i"] == COMMITS - 1

        code = srv.stop()
        assert code == 0
    finally:
        srv.cleanup()
