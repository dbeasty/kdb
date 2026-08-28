"""Shared fixtures for subprocess kdb CLI E2E tests."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from server_fixtures import cluster, server, tls_material  # noqa: F401  (pytest fixtures)

KDB_CLI_MAIN = "dev.kdb.cli.KdbCliKt"


def _java_executable() -> str:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / "java"
        if candidate.is_file():
            return str(candidate)
    found = shutil.which("java")
    if found:
        return found
    raise RuntimeError("java not found; set JAVA_HOME or ensure java is on PATH")


def _read_classpath() -> str:
    if os.environ.get("KDB_CLI_BIN"):
        # Go CLI runs don't need the JVM classpath at all - run_kdb never consults it when
        # KDB_CLI_BIN is set, so don't fail fixture setup over its absence.
        return ""
    path = os.environ.get("KDB_CLI_CLASSPATH_FILE")
    if not path:
        # Fall back to the Go CLI if it's built - the CLI test suite is CLI-agnostic. Only
        # skip when neither CLI is available at all.
        go_cli = Path(__file__).resolve().parents[2] / "go" / "bin" / "kdb"
        if go_cli.is_file():
            os.environ["KDB_CLI_BIN"] = str(go_cli)
            return ""
        pytest.skip(
            "no CLI available: set KDB_CLI_CLASSPATH_FILE (JVM, via "
            "./gradlew :kdb-integration:e2ePython) or KDB_CLI_BIN / make build-go (Go)")
    classpath_file = Path(path)
    if not classpath_file.is_file():
        raise RuntimeError(f"classpath file missing: {classpath_file}")
    return classpath_file.read_text().strip()


@pytest.fixture(scope="session")
def kdb_cli_classpath() -> str:
    return _read_classpath()


@pytest.fixture(scope="session")
def java_executable() -> str:
    return _java_executable()


@pytest.fixture
def data_dir() -> str:
    path = tempfile.mkdtemp(prefix="kdb-e2e-")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def run_kdb(
    java_executable: str,
    kdb_cli_classpath: str,
    data_dir: str,
    *args: str,
    quiet: bool = True,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    go_bin = os.environ.get("KDB_CLI_BIN")
    if go_bin:
        cmd = [go_bin, "--data-dir", data_dir]
        if quiet:
            cmd.append("--quiet")
        cmd.extend(args)
        return subprocess.run(
            cmd,
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
        )
    cmd = [
        java_executable,
        "-cp",
        kdb_cli_classpath,
        KDB_CLI_MAIN,
        "--data-dir",
        data_dir,
    ]
    if quiet:
        cmd.append("--quiet")
    cmd.extend(args)
    return subprocess.run(
        cmd,
        input=input_text,
        capture_output=True,
        text=True,
        check=False,
    )
