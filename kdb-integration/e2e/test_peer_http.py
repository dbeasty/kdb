"""Peer sync over WebSocket from an HTTP server (Phase 16.2, G2).

The cloud serves peer sync from an HTTP address (--peer-http, the same handler an application
mounts on its own router); the phone dials ws://.../kdb/sync. Both directions converge, and keep
converging across a kill -9 of the phone.
"""

from __future__ import annotations

import time

import pytest

from server_fixtures import KdbServer, free_port, get, put

pytestmark = [pytest.mark.server, pytest.mark.cluster]


def wait_for(what: str, cond, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def test_peer_sync_over_websocket_from_an_http_address():
    http_port = free_port()
    cloud = KdbServer(extra_args=["--peer-http", f"127.0.0.1:{http_port}"]).start()
    phone = None
    try:
        doc = "00000000000000000000000000000c01"
        put(cloud, doc, {"id": doc, "from": "cloud"})
        phone = KdbServer(extra_args=[
            "--peer", f"name=cloud,addr=ws://127.0.0.1:{http_port}/kdb/sync,interval=300ms",
        ]).start()
        try:
            wait_for("the cloud's write to reach the phone over WebSocket", lambda: get(phone, doc))
        except AssertionError:
            raise AssertionError("--- phone:\n" + phone.logs()[-5000:] + "\n--- cloud:\n" + cloud.logs()[-3000:])
        mine = "00000000000000000000000000000a01"
        put(phone, mine, {"id": mine, "from": "phone"})
        wait_for("the phone's write to reach the cloud", lambda: get(cloud, mine))

        phone.kill9()
        phone.restart()
        later = "00000000000000000000000000000c02"
        put(cloud, later, {"id": later, "after": "restart"})
        wait_for("a later write to reach the restarted phone", lambda: get(phone, later))
    finally:
        for srv in (phone, cloud):
            if srv is None:
                continue
            try:
                srv.stop()
            finally:
                srv.cleanup()
