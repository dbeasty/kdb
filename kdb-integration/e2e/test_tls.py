"""E2e scenario 7 (Phase 3.2): TLS over the real wire.

Go client <-> Go TLS server: handshake succeeds with the right CA, an untrusting client is
rejected, a plaintext client cannot talk to a TLS listener, and peer sync works over TLS. The
cross-language halves (Go client <-> Kotlin TLS server and the reverse) are interop-marked
prerequisites tracked in the plan: Kotlin's production listener is WS-only and Go has no WS
server yet, and the PKCS12/PEM fixture bridge is not built.
"""

from __future__ import annotations

import pytest

from server_fixtures import KdbServer, get, helper, put, relay

pytestmark = pytest.mark.server


@pytest.fixture
def tls_server(tls_material):
    srv = KdbServer(tls=tls_material).start()
    yield srv
    code = srv.stop()
    srv.cleanup()
    assert code == 0


DOC = "00000000000000000000000000000cc1"


def test_tls_round_trip(tls_server, tls_material):
    commit = put(tls_server, DOC, {"id": DOC, "secure": True}, tls=tls_material)
    assert len(commit) == 64
    assert get(tls_server, DOC, tls=tls_material)["secure"] is True


def test_client_without_ca_rejected(tls_server):
    # No --tls-ca at all: the helper tries plaintext against a TLS listener (tcps scheme is
    # required for TLS; with no TLS flags the address still says tcps -> connect must fail
    # loudly, not silently downgrade).
    r = helper("get", "--addr", tls_server.sql_addr, "--namespace", tls_server.namespace,
               "--doc-id", DOC, check=False)
    assert r.returncode != 0, "TLS-less client talked to a TLS listener"


def test_plaintext_scheme_against_tls_listener_fails(tls_server, tls_material):
    plaintext_addr = tls_server.sql_addr.replace("tcps://", "tcp://")
    r = helper("get", "--addr", plaintext_addr, "--namespace", tls_server.namespace,
               "--doc-id", DOC, check=False, timeout=30)
    assert r.returncode != 0, "plaintext connection succeeded against a TLS listener"


def test_peer_sync_over_tls(tls_server, tls_material):
    put(tls_server, DOC, {"id": DOC, "via": "tls"}, tls=tls_material)
    r = helper("relay", "--namespace", tls_server.namespace,
               "--servers", tls_server.peer_addr, "--rounds", "1", tls=tls_material)
    assert "applied=1" in r.stdout, r.stdout


@pytest.mark.interop
def test_go_client_to_kotlin_tls_server():
    pytest.skip(
        "prerequisites tracked in docs/kdb-finish-up-plan.md Phase 2.1 notes: Kotlin's "
        "production TLS listener is WS-only (Go client would need wss:// against a JVM server "
        "fixture) and the shared PKCS12/PEM certificate fixture generation is not built yet")
