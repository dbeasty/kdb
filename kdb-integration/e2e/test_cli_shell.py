"""CLI interactive shell via stdin pipe."""

from __future__ import annotations

from conftest import run_kdb

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
