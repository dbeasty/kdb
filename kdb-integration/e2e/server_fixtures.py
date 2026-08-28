"""Server lifecycle fixtures for the e2e harness (kdb-finish-up-plan Phase 3.2).

KdbServer launches a real ``kdb-service`` binary (``KDB_SERVICE_BIN``, default
``go/bin/kdb-service``) with a temp ``--data-dir``, ephemeral ports, an admin endpoint polled
for readiness, and optional TLS/RBAC. Teardown SIGTERMs and asserts a clean exit;
``kill9()`` is for crash tests. ``cluster(n)`` starts n servers; peer-sync traffic between
them is driven by the ``kdb-e2e-helper`` binary (``KDB_E2E_HELPER_BIN``), which is also the
wire-level client (put/get/exec/query) for scenarios.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _binary(env_var: str, default_rel: str) -> str:
    override = os.environ.get(env_var)
    if override:
        return override
    candidate = REPO_ROOT / default_rel
    if candidate.is_file():
        return str(candidate)
    raise RuntimeError(
        f"{env_var} not set and {candidate} not built - run `make build-go` and "
        f"`go build -o go/bin/kdb-e2e-helper ./cmd/kdb-e2e-helper` (from go/) first"
    )


def service_bin() -> str:
    return _binary("KDB_SERVICE_BIN", "go/bin/kdb-service")


def helper_bin() -> str:
    return _binary("KDB_E2E_HELPER_BIN", "go/bin/kdb-e2e-helper")


def inspect_bin() -> str:
    return _binary("KDB_INSPECT_BIN", "go/bin/kdb-inspect")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class TlsMaterial:
    """A self-signed CA plus one server cert/key issued from it (session-scoped fixture)."""

    ca_cert: str
    server_cert: str
    server_key: str


def make_tls_material(tmpdir: str) -> TlsMaterial:
    """Generate a CA and a 127.0.0.1 server certificate with openssl."""
    ca_key = os.path.join(tmpdir, "ca.key")
    ca_cert = os.path.join(tmpdir, "ca.crt")
    srv_key = os.path.join(tmpdir, "server.key")
    srv_csr = os.path.join(tmpdir, "server.csr")
    srv_cert = os.path.join(tmpdir, "server.crt")
    ext = os.path.join(tmpdir, "san.ext")

    def run(*cmd: str) -> None:
        subprocess.run(cmd, check=True, capture_output=True)

    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", ca_key,
        "-out", ca_cert, "-days", "1", "-subj", "/CN=kdb-e2e-ca")
    run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", srv_key,
        "-out", srv_csr, "-subj", "/CN=127.0.0.1")
    Path(ext).write_text("subjectAltName=IP:127.0.0.1,DNS:localhost\n")
    run("openssl", "x509", "-req", "-in", srv_csr, "-CA", ca_cert, "-CAkey", ca_key,
        "-CAcreateserial", "-out", srv_cert, "-days", "1", "-extfile", ext)
    return TlsMaterial(ca_cert=ca_cert, server_cert=srv_cert, server_key=srv_key)


@dataclass
class KdbServer:
    namespace: str = "e2e/data"
    rbac: bool = False
    tls: TlsMaterial | None = None
    # bootstrap_users: [(user, password, roles-csv)]; bootstrap_roles: [(role, grants-csv)]
    bootstrap_users: list = field(default_factory=list)
    bootstrap_roles: list = field(default_factory=list)
    extra_args: list = field(default_factory=list)

    data_dir: str = ""
    sql_port: int = 0
    peer_port: int = 0
    stream_port: int = 0
    admin_port: int = 0
    proc: subprocess.Popen | None = None
    log_path: str = ""

    _bootstrapped: bool = False

    def start(self) -> "KdbServer":
        if not self.data_dir:
            self.data_dir = tempfile.mkdtemp(prefix="kdb-e2e-srv-")
        if self._bootstrapped:
            # restart() over an existing data dir: users/roles are already in the durable
            # registry (re-running `user create` would rightly fail on the duplicate).
            return self._launch()
        for role, grants in self.bootstrap_roles:
            subprocess.run(
                [service_bin(), "user", "role", "--data-dir", self.data_dir,
                 "--role", role, "--grants", grants],
                check=True, capture_output=True, text=True)
        for user, password, roles in self.bootstrap_users:
            subprocess.run(
                [service_bin(), "user", "create", "--data-dir", self.data_dir,
                 "--user", user, "--password", password, "--roles", roles],
                check=True, capture_output=True, text=True)
        self._bootstrapped = True
        return self._launch()

    def _launch(self) -> "KdbServer":
        self.sql_port = free_port()
        self.peer_port = free_port()
        self.stream_port = free_port()
        self.admin_port = free_port()
        self.log_path = os.path.join(self.data_dir, "service.log")
        cmd = [
            service_bin(),
            "--data-dir", self.data_dir,
            "--namespace", self.namespace,
            "--sql-addr", f"tcp://127.0.0.1:{self.sql_port}?bind=true",
            "--peer-addr", f"tcp://127.0.0.1:{self.peer_port}?bind=true",
            "--stream-addr", f"tcp://127.0.0.1:{self.stream_port}?bind=true",
            "--admin-addr", f"127.0.0.1:{self.admin_port}",
            "--log-format", "json",
            "--drain-timeout", "10s",
        ]
        if self.rbac:
            cmd.append("--rbac")
        if self.tls:
            cmd += ["--tls-cert", self.tls.server_cert, "--tls-key", self.tls.server_key]
        cmd += self.extra_args
        log = open(self.log_path, "w")
        self.proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, text=True)
        self.wait_ready()
        return self

    def wait_ready(self, timeout: float = 20.0) -> None:
        deadline = time.time() + timeout
        url = f"http://127.0.0.1:{self.admin_port}/readyz"
        while time.time() < deadline:
            if self.proc and self.proc.poll() is not None:
                raise RuntimeError(
                    f"kdb-service exited early ({self.proc.returncode}):\n{self.logs()}")
            try:
                with urllib.request.urlopen(url, timeout=1) as resp:
                    if resp.status == 200:
                        return
            except (urllib.error.URLError, ConnectionError, OSError):
                pass
            time.sleep(0.1)
        raise TimeoutError(f"server not ready after {timeout}s:\n{self.logs()}")

    # -- addresses ---------------------------------------------------------------------------
    @property
    def scheme(self) -> str:
        return "tcps" if self.tls else "tcp"

    @property
    def sql_addr(self) -> str:
        return f"{self.scheme}://127.0.0.1:{self.sql_port}"

    @property
    def peer_addr(self) -> str:
        return f"{self.scheme}://127.0.0.1:{self.peer_port}"

    @property
    def admin_url(self) -> str:
        return f"http://127.0.0.1:{self.admin_port}"

    def admin_get(self, path: str) -> tuple[int, str]:
        try:
            with urllib.request.urlopen(self.admin_url + path, timeout=5) as resp:
                return resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    def logs(self) -> str:
        try:
            return Path(self.log_path).read_text()
        except OSError:
            return "<no logs>"

    def log_lines(self) -> list[dict]:
        out = []
        for line in self.logs().splitlines():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return out

    # -- lifecycle ---------------------------------------------------------------------------
    def stop(self, timeout: float = 30.0) -> int:
        """SIGTERM and wait; returns the exit code (asserted 0 by the fixture teardown)."""
        if self.proc is None or self.proc.poll() is not None:
            return self.proc.returncode if self.proc else -1
        self.proc.send_signal(signal.SIGTERM)
        return self.proc.wait(timeout=timeout)

    def kill9(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)

    def restart(self) -> "KdbServer":
        """Start again over the same data dir (new ports)."""
        assert self.proc is None or self.proc.poll() is not None, "stop/kill first"
        return self.start()

    def cleanup(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.kill9()
        shutil.rmtree(self.data_dir, ignore_errors=True)


# -- helper-binary client calls --------------------------------------------------------------

def helper(
    *args: str,
    token: str = "",
    tls: TlsMaterial | None = None,
    check: bool = True,
    timeout: float = 60.0,
) -> subprocess.CompletedProcess:
    cmd = [helper_bin(), *args]
    if token:
        cmd += ["--token", token]
    if tls:
        cmd += ["--tls-ca", tls.ca_cert]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise AssertionError(
            f"helper {' '.join(args)} failed ({result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}")
    return result


def put(server: KdbServer, doc_id: str, body: dict, **kw) -> str:
    r = helper("put", "--addr", server.sql_addr, "--namespace", server.namespace,
               "--doc-id", doc_id, "--json", json.dumps(body), **kw)
    return r.stdout.strip()


def upsert(server: KdbServer, doc_id: str, body: dict, **kw) -> str:
    r = helper("upsert", "--addr", server.sql_addr, "--namespace", server.namespace,
               "--doc-id", doc_id, "--json", json.dumps(body), **kw)
    return r.stdout.strip()


def get(server: KdbServer, doc_id: str, **kw) -> dict | None:
    r = helper("get", "--addr", server.sql_addr, "--namespace", server.namespace,
               "--doc-id", doc_id, check=False, **kw)
    if r.returncode != 0:
        return None
    lines = r.stdout.strip().splitlines()
    return json.loads(lines[1]) if len(lines) > 1 and lines[1] else None


def sql_exec(server: KdbServer, sql: str, **kw) -> subprocess.CompletedProcess:
    return helper("exec", "--addr", server.sql_addr, "--namespace", server.namespace,
                  "--sql", sql, **kw)


def sql_query(server: KdbServer, sql: str, **kw) -> list[list[str]]:
    """Returns the raw string rows of a SELECT."""
    r = helper("query", "--addr", server.sql_addr, "--namespace", server.namespace,
               "--sql", sql, **kw)
    return json.loads(r.stdout).get("rows") or []


def relay(servers: list[KdbServer], rounds: int = 2, **kw) -> str:
    """Run the full-peer relay across the servers' peer-sync listeners."""
    uris = ",".join(s.peer_addr for s in servers)
    r = helper("relay", "--namespace", servers[0].namespace, "--servers", uris,
               "--rounds", str(rounds), **kw)
    return r.stdout


DOC_IDS = [f"{i:032x}" for i in range(1, 64)]


# -- pytest fixtures -------------------------------------------------------------------------

@pytest.fixture
def server():
    srv = KdbServer().start()
    yield srv
    code = srv.stop()
    srv.cleanup()
    assert code == 0, f"server did not shut down cleanly (exit {code}):\n{srv.logs()}"


@pytest.fixture
def cluster():
    servers: list[KdbServer] = []

    def _make(n: int, **kwargs) -> list[KdbServer]:
        for _ in range(n):
            servers.append(KdbServer(**kwargs).start())
        return servers

    yield _make
    for srv in servers:
        try:
            code = srv.stop()
            assert code == 0, f"cluster server exit {code}:\n{srv.logs()}"
        finally:
            srv.cleanup()


@pytest.fixture(scope="session")
def tls_material():
    tmpdir = tempfile.mkdtemp(prefix="kdb-e2e-tls-")
    yield make_tls_material(tmpdir)
    shutil.rmtree(tmpdir, ignore_errors=True)
