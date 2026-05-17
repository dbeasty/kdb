"""CLI usage and error exit codes."""

from __future__ import annotations

from conftest import run_kdb


def test_unknown_command_exit_2(
    java_executable: str,
    kdb_cli_classpath: str,
    data_dir: str,
) -> None:
    result = run_kdb(
        java_executable,
        kdb_cli_classpath,
        data_dir,
        "unknown-cmd",
        quiet=False,
    )
    assert result.returncode == 2
