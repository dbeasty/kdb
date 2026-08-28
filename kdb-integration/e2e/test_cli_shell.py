"""CLI interactive shell via stdin pipe."""

from __future__ import annotations

from conftest import run_kdb

import os

import pytest

# The JVM classpath being absent means the run will fall back to the Go CLI (see conftest's
# _read_classpath), which lacks this command.
pytestmark = pytest.mark.skipif(
    bool(os.environ.get("KDB_CLI_BIN")) or not os.environ.get("KDB_CLI_CLASSPATH_FILE"),
    reason="shell is not implemented in the Go CLI yet (kdb-finish-up-plan 4.E/4.H) - JVM CLI only",
)


DOC_ID = "00000000-0000-0000-0000-000000000002"


def test_shell_put_and_query(
    java_executable: str,
    kdb_cli_classpath: str,
    data_dir: str,
) -> None:
    assert (
        run_kdb(java_executable, kdb_cli_classpath, data_dir, "init", "app/users").returncode
        == 0
    )
    stdin = "\n".join(
        [
            f'put {{"id":"{DOC_ID}","userId":"u1"}}',
            "query SELECT _doc FROM users",
            "exit",
            "",
        ],
    )
    result = run_kdb(
        java_executable,
        kdb_cli_classpath,
        data_dir,
        "shell",
        "app/users",
        quiet=True,
        input_text=stdin,
    )
    assert result.returncode == 0, result.stderr
    assert "u1" in result.stdout
