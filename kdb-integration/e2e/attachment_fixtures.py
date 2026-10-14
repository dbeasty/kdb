"""Helpers for the attachment e2e tests: thin wrappers over the kdb-e2e-helper attachment commands
(go/cmd/kdb-e2e-helper/attachments.go) plus on-disk measurement of the attachment store."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

from server_fixtures import KdbServer, helper, helper_bin


def _run(cmd: str, server: KdbServer, *args: str, timeout: float = 120.0) -> dict:
    r = helper(cmd, "--addr", server.sql_addr, "--namespace", server.namespace,
               "--timeout", f"{int(timeout)}s", *args, timeout=timeout + 30)
    return json.loads(r.stdout)


def doc_id(n: int) -> str:
    return f"{n:032x}"


def attach(server: KdbServer, doc: str, name: str, path: str | Path, mime: str = "",
           drop_at: int = 0, commit_delay: float = 0, timeout: float = 120.0) -> dict:
    args = ["--doc-id", doc, "--name", name, "--file", str(path)]
    if commit_delay:
        args += ["--commit-delay", f"{commit_delay}s"]
    if mime:
        args += ["--mime", mime]
    if drop_at:
        args += ["--drop-at", str(drop_at)]
    return _run("attach", server, *args, timeout=timeout)


def tx_attach(server: KdbServer, doc: str, body: dict, name: str, path: str | Path,
              mime: str = "", timeout: float = 120.0) -> dict:
    args = ["--doc-id", doc, "--json", json.dumps(body), "--name", name, "--file", str(path)]
    if mime:
        args += ["--mime", mime]
    return _run("tx-attach", server, *args, timeout=timeout)


def sql_attach(server: KdbServer, sql: str, path: str | Path, timeout: float = 120.0) -> dict:
    return _run("sql-attach", server, "--sql", sql, "--file", str(path), timeout=timeout)


def read_attachment(server: KdbServer, doc: str, name: str = "", attachment_id: str = "",
                    as_of: str = "", out: str | Path | None = None, timeout: float = 120.0,
                    offset: int | None = None, length: int | None = None, resume_at: int = 0,
                    pause_files: tuple[Path, Path] | None = None) -> dict:
    """Streams the attachment through the Go client and returns {sha256, size, hash, commit,
    superseded, ...}, or {"error", "kind"} for an expected failure (not_found, corrupt, range).
    offset/length make it a verified ranged read; resume_at reads that many bytes, drops the
    connection, reconnects and resumes the same version (pause_files: ready/go handshake while
    disconnected)."""
    args = ["--doc-id", doc]
    if offset is not None:
        args += ["--offset", str(offset)]
    if length is not None:
        args += ["--length", str(length)]
    if resume_at:
        args += ["--resume-at", str(resume_at)]
    if pause_files:
        args += ["--ready-file", str(pause_files[0]), "--go-file", str(pause_files[1])]
    args += ["--attachment-id", attachment_id] if attachment_id else ["--name", name]
    if as_of:
        args += ["--as-of", as_of]
    if out:
        args += ["--out", str(out)]
    return _run("read-attachment", server, *args, timeout=timeout)


def list_attachments(server: KdbServer, doc: str, as_of: str = "") -> dict:
    args = ["--doc-id", doc] + (["--as-of", as_of] if as_of else [])
    return _run("list-attachments", server, *args)


class PausedRead:
    """A read that streams pause_after bytes and then stops until resumed - for racing a long
    download against commits, maintenance and GC."""

    def __init__(self, server: KdbServer, doc: str, name: str, pause_after: int, workdir: Path,
                 out: Path | None = None, timeout: float = 300.0):
        self.ready, self.go = workdir / "read.ready", workdir / "read.go"
        for p in (self.ready, self.go):
            p.unlink(missing_ok=True)
        cmd = [helper_bin(), "read-attachment", "--addr", server.sql_addr, "--namespace",
               server.namespace, "--doc-id", doc, "--name", name, "--pause-after", str(pause_after),
               "--ready-file", str(self.ready), "--go-file", str(self.go),
               "--timeout", f"{int(timeout)}s"]
        if out:
            cmd += ["--out", str(out)]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + 60
        while not self.ready.exists():
            if self.proc.poll() is not None or time.time() > deadline:
                _, err = self.proc.communicate(timeout=10)
                raise AssertionError(f"the read never paused: {err}")
            time.sleep(0.05)

    def resume(self, timeout: float = 300.0) -> dict:
        self.go.write_text("go\n")
        out, err = self.proc.communicate(timeout=timeout)
        if self.proc.returncode != 0:
            raise AssertionError(f"paused read failed ({self.proc.returncode}): {err}")
        return json.loads(out)


def attachment_dir(server: KdbServer) -> Path:
    return Path(server.data_dir) / "ns" / server.namespace / "attachment"


def stored_bytes(server: KdbServer) -> int:
    """Bytes in the namespace's attachment store segments (pointers, bytes, leaves, deltas)."""
    d = attachment_dir(server)
    if not d.exists():
        return 0
    return sum(p.stat().st_size for p in d.rglob("*") if p.is_file())


def wait_stored_settles(server: KdbServer, below: int | None = None, timeout: float = 60.0,
                        quiet: float = 2.0) -> int:
    """Waits for background maintenance to stop changing the attachment store (and, with below,
    for it to drop under that size); returns the settled size."""
    deadline = time.time() + timeout
    last, since = stored_bytes(server), time.time()
    while time.time() < deadline:
        time.sleep(0.25)
        now = stored_bytes(server)
        if now != last:
            last, since = now, time.time()
            continue
        if time.time() - since >= quiet and (below is None or now < below):
            return now
    raise AssertionError(f"attachment store did not settle (now {last} bytes, wanted < {below})")


def table(server: KdbServer) -> str:
    return server.namespace.split("/")[-1]


def wait_for(what: str, cond, timeout: float = 60.0, on_fail=lambda: "") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}\n{on_fail()}")


def maintained_server(**kw) -> KdbServer:
    """A server whose maintenance loop runs every 300 ms, so deltas and GC happen within a test."""
    extra = kw.pop("extra_args", [])
    return KdbServer(extra_args=["--maintenance-interval", "300ms", "--maintenance-sweep", "1s",
                                 *extra], **kw)


def file_size(path: str | Path) -> int:
    return os.path.getsize(path)
