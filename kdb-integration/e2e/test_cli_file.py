"""CLI file attachment subprocess round-trip."""

from __future__ import annotations

import tempfile
from pathlib import Path

from conftest import run_kdb

FILE_ID = "00000000-0000-0000-0000-0000000000aa"


def test_file_put_get_roundtrip(
    java_executable: str,
    kdb_cli_classpath: str,
    data_dir: str,
) -> None:
    payload = bytes([9, 8, 7, 6, 5])
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "payload.bin"
        out = Path(tmp) / "out.bin"
        src.write_bytes(payload)

        assert (
            run_kdb(
                java_executable,
                kdb_cli_classpath,
                data_dir,
                "init",
                "app/files",
            ).returncode
            == 0
        )
        put = run_kdb(
            java_executable,
            kdb_cli_classpath,
            data_dir,
            "file",
            "put",
            "app/files",
            "--id",
            FILE_ID,
            str(src),
        )
        assert put.returncode == 0, put.stderr
        get = run_kdb(
            java_executable,
            kdb_cli_classpath,
            data_dir,
            "file",
            "get",
            "app/files",
            "--id",
            FILE_ID,
            "-o",
            str(out),
        )
        assert get.returncode == 0, get.stderr
        assert out.read_bytes() == payload
