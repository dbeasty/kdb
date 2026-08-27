"""E2e scenario 8 (Phase 3.2): sustained load past flush thresholds; reads stay correct.

Pushes enough write volume through the wire (~6MB of document payloads across repeated
overwrites) to exercise segment growth and flush under load, interleaving reads that must
always see the latest acknowledged value, then restarts and re-checks.
"""

from __future__ import annotations

import pytest

from server_fixtures import KdbServer, get, helper, put

pytestmark = [pytest.mark.server, pytest.mark.slow]


def test_sustained_load_reads_correct_during_and_after():
    srv = KdbServer().start()
    try:
        # Interleave: bursts of load, then spot reads of a doc we control.
        probe = "00000000000000000000000000000f01"
        for burst in range(5):
            r = helper("load", "--addr", srv.sql_addr, "--namespace", srv.namespace,
                       "--rounds", "400", timeout=300)
            assert "loaded=400" in r.stdout
            put(srv, probe, {"id": probe, "burst": burst})
            body = get(srv, probe)
            assert body is not None and body["burst"] == burst, (
                f"read during load returned stale/missing data at burst {burst}")

        code, metrics = srv.admin_get("/metrics")
        assert code == 200
        # The write path actually recorded work (fsync/lock-wait stages under real load).
        assert "kdb_stage_ops_total" in metrics

        code = srv.stop(timeout=60)
        assert code == 0
        srv.restart()
        body = get(srv, probe)
        assert body is not None and body["burst"] == 4
        code = srv.stop()
        assert code == 0
    finally:
        srv.cleanup()
