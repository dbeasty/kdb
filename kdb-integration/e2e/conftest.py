"""Shared fixtures for subprocess kdb CLI E2E tests."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

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
    path = os.environ.get("KDB_CLI_CLASSPATH_FILE")
    if not path:
        raise RuntimeError(
            "KDB_CLI_CLASSPATH_FILE is not set; run via ./gradlew :kdb-integration:e2ePython",
        )
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
