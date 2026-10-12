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


def prev_service_bin() -> str | None:
    """The previous release's kdb-service (``KDB_SERVICE_BIN_PREV``) for the mixed-version
    upgrade tests, or None when it is not set. Build it with `make build-prev-service`
    (scripts/build-prev-service.sh); tests that need it skip without it."""
    path = os.environ.get("KDB_SERVICE_BIN_PREV")
    if not path:
        return None
    if not os.path.isfile(path):
        raise RuntimeError(f"KDB_SERVICE_BIN_PREV={path} does not exist")
    return path


_flag_cache: dict[str, frozenset[str]] = {}


def supported_flags(binary: str) -> frozenset[str]:
    """The flags a kdb-service binary accepts, read from its --help once per binary.

    The harness drives binaries of different releases with one launch command. A flag a binary
    does not know is a startup failure ("flag provided but not defined"), so it is caught before
    the launch with a message naming the binary and its version instead. v0.6.2 accepts every flag
    the fixtures pass; the only flag main has added since is --max-result-rows."""
    cached = _flag_cache.get(binary)
    if cached is None:
        r = subprocess.run([binary, "--help"], capture_output=True, text=True, timeout=30)
        names = set()
        for line in (r.stdout + r.stderr).splitlines():
            stripped = line.strip()
            if stripped.startswith("-"):
                names.add(stripped.lstrip("-").split()[0].split("=")[0])
        cached = _flag_cache[binary] = frozenset(names)
    return cached


def binary_version(binary: str) -> str:
    """The first line of `<binary> --version`, e.g. "0.6.2 (commit afa9351, ...)"."""
    r = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=30)
    return ((r.stdout or r.stderr).strip().splitlines() or ["?"])[0]


def helper_bin() -> str:
    return _binary("KDB_E2E_HELPER_BIN", "go/bin/kdb-e2e-helper")


def inspect_bin() -> str:
    return _binary("KDB_INSPECT_BIN", "go/bin/kdb-inspect")


def free_port() -> int:
    return free_ports(1)[0]


def free_ports(count: int) -> list[int]:
    """Reserves `count` distinct ephemeral ports.

    Every socket is held open until all of them are bound, then they are all closed. Allocating
    them one at a time - bind, read the port, close, repeat - lets the kernel hand the same port
    out again on the next call, because closing returns it to the ephemeral pool immediately. Four
    sequential calls were four chances to hand the same number to two listeners, which surfaced in
    CI as `admin listen ...: bind: address already in use` and took the whole e2e job with it.

    This narrows the race rather than closing it: there is still a window between the last close
    and the service binding, in which anything on the machine could take the port. Nothing short of
    passing pre-bound sockets to the child can remove that, and the service takes addresses on the
    command line. `launch_with_retry` covers what remains.
    """
    socks = []
    try:
        for _ in range(count):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


# Pinned ports come from below every OS's ephemeral range (Linux starts at 32768 by default, macOS
# at 49152). A port the kernel handed out as ephemeral is also fair game as the *source* port of
# any outgoing connection, so while a killed node is down one of the many client connections a
# cluster test makes can take its port, and the node cannot bind it again on restart - which is
# exactly what happened on a Linux runner and never on a laptop.
_STABLE_PORT_RANGE = (20000, 32000)
_stable_ports_taken: set[int] = set()


def stable_ports(count: int) -> list[int]:
    """Reserves `count` distinct ports outside the ephemeral range, for servers that must come back
    on the same ports after a kill. Each is checked by binding it, and never handed out twice in
    this process."""
    import random
    rng = random.Random()
    lo, hi = _STABLE_PORT_RANGE
    socks, ports = [], []
    try:
        while len(ports) < count:
            port = rng.randrange(lo, hi)
            if port in _stable_ports_taken or port in ports:
                continue
            s = socket.socket()
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                s.close()
                continue
            socks.append(s)
            ports.append(port)
        _stable_ports_taken.update(ports)
        return ports
    finally:
        for s in socks:
            s.close()


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
    # control: also serve the /v1 control plane (--control-addr), which cluster tests read heads,
    # documents, conflicts and peer progress from; control_write enables its mutating endpoints.
    control: bool = False
    control_write: bool = False
    # pin_ports: keep the same ports across restarts. Peers name each other by address in their
    # --peer specs, so a node that came back on new ports would silently drop out of the cluster.
    pin_ports: bool = False
    # service_bin: the kdb-service binary this server runs; empty means today's service_bin().
    # restart(service_bin=...) switches it on the same data dir and (with pin_ports) the same
    # ports - how the upgrade tests move a node from one release to the next.
    service_bin: str = ""

    data_dir: str = ""
    sql_port: int = 0
    peer_port: int = 0
    stream_port: int = 0
    admin_port: int = 0
    control_port: int = 0
    proc: subprocess.Popen | None = None
    log_path: str = ""
    launches: int = 0

    _bootstrapped: bool = False

    @property
    def binary(self) -> str:
        return self.service_bin or service_bin()

    def start(self) -> "KdbServer":
        if not self.data_dir:
            self.data_dir = tempfile.mkdtemp(prefix="kdb-e2e-srv-")
        if self._bootstrapped:
            # restart() over an existing data dir: users/roles are already in the durable
            # registry (re-running `user create` would rightly fail on the duplicate).
            return self._launch()
        for role, grants in self.bootstrap_roles:
            subprocess.run(
                [self.binary, "user", "role", "--data-dir", self.data_dir,
                 "--role", role, "--grants", grants],
                check=True, capture_output=True, text=True)
        for user, password, roles in self.bootstrap_users:
            subprocess.run(
                [self.binary, "user", "create", "--data-dir", self.data_dir,
                 "--user", user, "--password", password, "--roles", roles],
                check=True, capture_output=True, text=True)
        self._bootstrapped = True
        return self._launch()

    def reserve_ports(self) -> "KdbServer":
        """Allocates this server's ports now, so its addresses are known before it starts -
        what a cluster needs to write every node's --peer specs up front."""
        if not self.sql_port:
            alloc = stable_ports if self.pin_ports else free_ports
            (self.sql_port, self.peer_port, self.stream_port, self.admin_port,
             self.control_port) = alloc(5)
        return self

    def _launch(self, attempts: int = 4) -> "KdbServer":
        """Starts the service, retrying a port collision.

        A port this fixture reserved can still be taken between reserving it and the service
        binding it - most often by a service from an earlier test that was killed with SIGKILL and
        whose socket the kernel has not finished reclaiming. That is an artefact of how the test
        harness allocates ports, not something the server did wrong, so it is retried with a fresh
        set rather than failing the run. Any other early exit is a real failure and is raised.
        """
        last: RuntimeError | None = None
        if self.pin_ports:
            # The same ports or nothing: retry for longer, since what holds a pinned port is
            # normally this server's own previous process, killed a moment ago.
            attempts = max(attempts, 25)
        for attempt in range(attempts):
            try:
                return self._launch_once()
            except RuntimeError as e:
                if "address already in use" not in str(e):
                    raise
                last = e
                # Give the kernel a moment to reclaim whatever is holding it before trying again.
                time.sleep(0.2 * min(attempt + 1, 5))
        raise RuntimeError(
            f"kdb-service could not bind a free port in {attempts} attempts; "
            f"last failure:\n{last}")

    def _launch_once(self) -> "KdbServer":
        # One allocation for all of them, so the kernel cannot hand the same port to two.
        if not self.pin_ports:
            (self.sql_port, self.peer_port, self.stream_port, self.admin_port,
             self.control_port) = free_ports(5)
        elif not self.sql_port:
            self.reserve_ports()
        # One log per launch: reopening a single file with "w" on restart erased the log of the
        # run that was killed, which is the one a crash test needs to read.
        self.launches += 1
        self.log_path = os.path.join(self.data_dir, f"service.{self.launches}.log")
        cmd = [
            self.binary,
            "--data-dir", self.data_dir,
            "--namespace", self.namespace,
            "--sql-addr", f"tcp://127.0.0.1:{self.sql_port}?bind=true",
            "--peer-addr", f"tcp://127.0.0.1:{self.peer_port}?bind=true",
            "--stream-addr", f"tcp://127.0.0.1:{self.stream_port}?bind=true",
            "--admin-addr", f"127.0.0.1:{self.admin_port}",
            "--log-format", "json",
            "--drain-timeout", "10s",
        ]
        if self.control:
            cmd += ["--control-addr", f"127.0.0.1:{self.control_port}", "--control-ui=false"]
            if self.control_write:
                cmd.append("--control-write")
        if self.rbac:
            cmd.append("--rbac")
        if self.tls:
            cmd += ["--tls-cert", self.tls.server_cert, "--tls-key", self.tls.server_key]
        cmd += self.extra_args
        unknown = sorted({a[2:].split("=")[0] for a in cmd[1:] if a.startswith("--")}
                         - supported_flags(self.binary))
        if unknown:
            raise RuntimeError(f"{self.binary} ({binary_version(self.binary)}) does not accept "
                               f"{', '.join('--' + f for f in unknown)}")
        log = open(self.log_path, "w")
        # Not JSON, so log_lines() skips it: which binary this launch ran, for the evidence.
        log.write(f"# launch {self.launches}: {self.binary}\n")
        log.flush()
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
        """The current launch's log."""
        try:
            return Path(self.log_path).read_text()
        except OSError:
            return "<no logs>"

    def all_logs(self) -> str:
        """Every launch's log, oldest first - what to read after a kill and restart."""
        parts = []
        for n in range(1, self.launches + 1):
            path = os.path.join(self.data_dir, f"service.{n}.log")
            try:
                parts.append(f"--- launch {n} ---\n" + Path(path).read_text())
            except OSError:
                pass
        return "\n".join(parts) or "<no logs>"

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

    def restart(self, service_bin: str | None = None) -> "KdbServer":
        """Start again over the same data dir (new ports unless pin_ports), optionally switching
        to a different kdb-service binary first."""
        assert self.proc is None or self.proc.poll() is not None, "stop/kill first"
        if service_bin is not None:
            self.service_bin = service_bin
        return self.start()

    def cleanup(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.kill9()
        shutil.rmtree(self.data_dir, ignore_errors=True)


# -- failure evidence ------------------------------------------------------------------------

EVIDENCE_DIR = Path(os.environ.get("KDB_E2E_EVIDENCE_DIR", Path(__file__).parent / "evidence"))


def preserve_evidence(name: str, servers: list["KdbServer"], notes: str = "") -> str:
    """Copies each server's data directory - delta log, checkpoints, every launch's log - plus
    `notes` to EVIDENCE_DIR/<name>, so a failure that only shows up in CI can be examined after the
    runner is gone (CI uploads that directory when a job fails). Returns the path."""
    dest = EVIDENCE_DIR / f"{name}-{int(time.time())}"
    dest.mkdir(parents=True, exist_ok=True)
    for i, srv in enumerate(servers):
        if srv.data_dir and os.path.isdir(srv.data_dir):
            try:
                shutil.copytree(srv.data_dir, dest / f"node{i}", dirs_exist_ok=True)
            except shutil.Error as e:
                # The node is usually still running, so a temp file (peers/*.json.tmp) can vanish
                # between listing and copying. Keep everything else rather than failing teardown
                # with this error in place of the test's own.
                notes += f"\n\nnode{i}: files that changed during the copy: {e}"
    # A node that is up but stuck (a held lock, a blocked commit) shows nothing in its logs; its
    # goroutines say who holds what. The admin port serves them, so keep them with the evidence.
    for i, srv in enumerate(servers):
        try:
            code, dump = srv.admin_get("/debug/pprof/goroutine?debug=2")
            if code == 200:
                (dest / f"node{i}").mkdir(parents=True, exist_ok=True)
                (dest / f"node{i}" / "goroutines.txt").write_text(dump)
        except Exception:
            pass  # the node is down or unreachable; its logs are already copied
    if notes:
        (dest / "notes.txt").write_text(notes)
    return str(dest)


def inspect_report(srv: "KdbServer") -> str:
    """kdb-inspect's verify and segment listing for a server's namespace, as text."""
    out = []
    for args in (["verify", "--codec", "zstd"], ["verify", "--codec", "zstd", "--level", "L2"]):
        try:
            r = subprocess.run([inspect_bin(), args[0], "--data-dir", srv.data_dir,
                                "--namespace", srv.namespace, *args[1:]],
                               capture_output=True, text=True, timeout=60)
            out.append(f"$ kdb-inspect {' '.join(args)} (exit {r.returncode})\n{r.stdout}{r.stderr}")
        except Exception as e:  # evidence gathering must not mask the real failure
            out.append(f"$ kdb-inspect {' '.join(args)}: {e}")
    return "\n".join(out)


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


def read(server: KdbServer, doc_id: str, **kw) -> tuple[str, dict | None]:
    """A point read that tells "not there" from "the read failed": returns (commit served at,
    body or None when the document does not exist) and raises, with the helper's stderr, on any
    failure. Prefer this to get() wherever an absent document would fail the test."""
    r = helper("get", "--addr", server.sql_addr, "--namespace", server.namespace,
               "--doc-id", doc_id, check=True, **kw)
    lines = r.stdout.split("\n")
    commit = lines[0].strip() if lines else ""
    body = lines[1].strip() if len(lines) > 1 else ""
    return commit, (json.loads(body) if body else None)


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
