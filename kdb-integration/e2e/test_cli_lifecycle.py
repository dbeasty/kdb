"""CLI lifecycle: init, put, get, query across separate subprocess invocations."""

from __future__ import annotations

from pathlib import Path

from conftest import run_kdb

DOC_ID = "00000000-0000-0000-0000-000000000001"


def test_init_creates_namespace_meta(
    java_executable: str,
    kdb_cli_classpath: str,
    data_dir: str,
) -> None:
    result = run_kdb(java_executable, kdb_cli_classpath, data_dir, "init", "app/demo")
    assert result.returncode == 0, result.stderr
    meta = Path(data_dir) / "ns" / "app" / "demo" / "meta.json"
    assert meta.is_file()


def test_put_get_survives_reopen(
    java_executable: str,
    kdb_cli_classpath: str,
    data_dir: str,
) -> None:
    assert (
        run_kdb(java_executable, kdb_cli_classpath, data_dir, "init", "app/t").returncode
        == 0
    )
    payload = f'{{"id":"{DOC_ID}","v":1}}'
    put = run_kdb(
        java_executable,
        kdb_cli_classpath,
        data_dir,
        "put",
        "app/t",
        payload,
    )
    assert put.returncode == 0, put.stderr
    get = run_kdb(
        java_executable,
        kdb_cli_classpath,
        data_dir,
        "get",
        "app/t",
        DOC_ID,
    )
    assert get.returncode == 0, get.stderr
    assert '"v":1' in get.stdout


def test_query_returns_row(
    java_executable: str,
    kdb_cli_classpath: str,
    data_dir: str,
) -> None:
    assert (
        run_kdb(java_executable, kdb_cli_classpath, data_dir, "init", "app/users").returncode
        == 0
    )
    payload = f'{{"id":"{DOC_ID}","userId":"u1"}}'
    put = run_kdb(
        java_executable,
        kdb_cli_classpath,
        data_dir,
        "put",
        "app/users",
        payload,
    )
    assert put.returncode == 0, put.stderr
    query = run_kdb(
        java_executable,
        kdb_cli_classpath,
        data_dir,
        "query",
        "app/users",
        "SELECT _doc FROM users",
    )
    assert query.returncode == 0, query.stderr
    assert DOC_ID in query.stdout or "u1" in query.stdout
