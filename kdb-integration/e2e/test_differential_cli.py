"""E2e scenario 11 (Phase 3.2): differential Go-vs-JVM CLI runs.

The same scripted scenario is executed once with the Go CLI and once with the JVM CLI, each
against its own data dir, and the observable outputs are compared; then the JVM CLI's data dir
is read back with the Go CLI (on-disk cross-language compatibility, the e2e-level extension of
go/kdb/interop's TestKotlinPutThenGoGet_InteropDelta). Requires the JVM classpath, so
interop-marked: run via `pytest -m interop` with KDB_CLI_CLASSPATH_FILE set (the
:kdb-integration:e2ePython Gradle path).
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path

import pytest

from conftest import KDB_CLI_MAIN

pytestmark = pytest.mark.interop

DOC_ID = "00000000000000000000000000000e11"
PAYLOAD = f'{{"id":"{DOC_ID}","userId":"diff-user","n":42}}'

REPO_ROOT = Path(__file__).resolve().parents[2]


def _go_cli() -> str:
    p = os.environ.get("KDB_CLI_BIN") or str(REPO_ROOT / "go" / "bin" / "kdb")
    if not Path(p).is_file():
        pytest.skip("Go CLI not built (make build-go)")
    return p


def _jvm_cli_cmd() -> list[str]:
    cp_file = os.environ.get("KDB_CLI_CLASSPATH_FILE")
    if not cp_file or not Path(cp_file).is_file():
        pytest.skip("KDB_CLI_CLASSPATH_FILE not set - run via ./gradlew :kdb-integration:e2ePython")
    java = os.environ.get("JAVA_HOME")
    java_bin = str(Path(java) / "bin" / "java") if java else "java"
    return [java_bin, "-cp", Path(cp_file).read_text().strip(), KDB_CLI_MAIN]


def run_cli(base: list[str], data_dir: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run([*base, "--data-dir", data_dir, "--quiet", *args],
                          capture_output=True, text=True, timeout=120)


SCRIPT = [
    ("init", "app/users"),
    ("put", "app/users", PAYLOAD),
    ("get", "app/users", DOC_ID),
    ("query", "app/users", "SELECT _doc FROM users"),
]


def observable(results: list[subprocess.CompletedProcess]) -> list[tuple[int, str]]:
    """The comparable surface: exit codes, and get/query payload content."""
    out = []
    for r in results:
        # Normalize: both CLIs print the document JSON for get; query output formats differ
        # (tab table vs. JVM's format), so compare on payload presence rather than layout.
        has_payload = "diff-user" in r.stdout
        out.append((r.returncode, "payload" if has_payload else r.stdout.strip()[:40]))
    return out


def test_same_script_same_observable_behavior():
    go_dir = tempfile.mkdtemp(prefix="kdb-diff-go-")
    jvm_dir = tempfile.mkdtemp(prefix="kdb-diff-jvm-")
    go_results = [run_cli([_go_cli()], go_dir, *step) for step in SCRIPT]
    jvm_results = [run_cli(_jvm_cli_cmd(), jvm_dir, *step) for step in SCRIPT]

    assert observable(go_results) == observable(jvm_results), (
        f"differential mismatch:\nGo:  {[(r.returncode, r.stdout, r.stderr) for r in go_results]}\n"
        f"JVM: {[(r.returncode, r.stdout, r.stderr) for r in jvm_results]}")


def test_jvm_writes_go_reads_on_disk():
    jvm_dir = tempfile.mkdtemp(prefix="kdb-diff-xlang-")
    jvm = _jvm_cli_cmd()
    assert run_cli(jvm, jvm_dir, "init", "app/users").returncode == 0
    put = run_cli(jvm, jvm_dir, "put", "app/users", PAYLOAD)
    assert put.returncode == 0, put.stderr

    got = run_cli([_go_cli()], jvm_dir, "get", "app/users", DOC_ID)
    assert got.returncode == 0, f"Go CLI cannot read the JVM-written dir: {got.stderr}"
    body = json.loads(got.stdout.strip())
    assert body["userId"] == "diff-user" and body["n"] == 42


def test_go_writes_jvm_reads_on_disk():
    go_dir = tempfile.mkdtemp(prefix="kdb-diff-xlang2-")
    go = [_go_cli()]
    assert run_cli(go, go_dir, "init", "app/users").returncode == 0
    put = run_cli(go, go_dir, "put", "app/users", PAYLOAD)
    assert put.returncode == 0, put.stderr

    got = run_cli(_jvm_cli_cmd(), go_dir, "get", "app/users", DOC_ID)
    assert got.returncode == 0, f"JVM CLI cannot read the Go-written dir: {got.stderr}"
    body = json.loads(got.stdout.strip())
    assert body["userId"] == "diff-user" and body["n"] == 42
