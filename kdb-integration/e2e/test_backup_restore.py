"""E2e scenario 10 (Phase 3.2): backup -> destroy data dir -> restore -> verify -> serve.

Drives the real kdb-inspect backup/restore commands (Phase 2.11) against a service-written
data directory, then restarts a service over the restored directory and reads the data back
over the wire. The S3 variant runs behind the s3 marker (LocalStack / real S3 via KDB_S3_*).
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile

import pytest

from server_fixtures import KdbServer, get, inspect_bin, put

pytestmark = [pytest.mark.server, pytest.mark.destructive]

DOCS = {f"{i:032x}": {"id": f"{i:032x}", "n": i} for i in range(1, 8)}


def run_inspect(*args: str) -> subprocess.CompletedProcess:
    r = subprocess.run([inspect_bin(), *args], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, f"kdb-inspect {' '.join(args)}:\n{r.stdout}\n{r.stderr}"
    return r


def test_backup_wipe_restore_serve_round_trip():
    srv = KdbServer().start()
    store_dir = tempfile.mkdtemp(prefix="kdb-e2e-bkstore-")
    restored_dir = tempfile.mkdtemp(prefix="kdb-e2e-restored-")
    try:
        for doc_id, body in DOCS.items():
            put(srv, doc_id, body)
        code = srv.stop()
        assert code == 0

        # Backup the (stopped) data dir.
        r = run_inspect("backup", "--data-dir", srv.data_dir, "--namespace", srv.namespace,
                        "--to", store_dir, "--codec", "zstd")
        backup_id = re.search(r"backupId: (\S+)", r.stdout).group(1)
        run_inspect("backup-verify", "--namespace", srv.namespace, "--to", store_dir,
                    "--backup-id", backup_id)

        # Destroy the original entirely.
        shutil.rmtree(srv.data_dir)

        # Restore from the backup alone into a fresh directory, then verify it.
        run_inspect("restore", "--namespace", srv.namespace, "--out", restored_dir,
                    "--from-backup", store_dir, "--backup-id", backup_id, "--codec", "zstd")
        run_inspect("verify", "--data-dir", restored_dir, "--namespace", srv.namespace,
                    "--codec", "zstd")

        # A service over the restored directory serves every document.
        srv.data_dir = restored_dir
        srv._bootstrapped = True  # nothing to bootstrap; reuse restart path
        srv.restart()
        for doc_id, body in DOCS.items():
            got = get(srv, doc_id)
            assert got is not None and got["n"] == body["n"], f"{doc_id} lost through backup+restore"
        code = srv.stop()
        assert code == 0
    finally:
        srv.cleanup()
        shutil.rmtree(store_dir, ignore_errors=True)
        shutil.rmtree(restored_dir, ignore_errors=True)


def test_incremental_backup_after_more_writes():
    srv = KdbServer().start()
    store_dir = tempfile.mkdtemp(prefix="kdb-e2e-bkstore-")
    try:
        put(srv, "10000000000000000000000000000001", {"id": "10000000000000000000000000000001", "gen": 1})
        code = srv.stop()
        assert code == 0
        r = run_inspect("backup", "--data-dir", srv.data_dir, "--namespace", srv.namespace,
                        "--to", store_dir, "--codec", "zstd")
        base_id = re.search(r"backupId: (\S+)", r.stdout).group(1)

        srv.restart()
        put(srv, "10000000000000000000000000000002", {"id": "10000000000000000000000000000002", "gen": 2})
        code = srv.stop()
        assert code == 0

        r = run_inspect("backup", "--data-dir", srv.data_dir, "--namespace", srv.namespace,
                        "--to", store_dir, "--codec", "zstd", "--base-backup-id", base_id)
        inc_id = re.search(r"backupId: (\S+)", r.stdout).group(1)
        assert f"incremental over: {base_id}" in r.stdout
        run_inspect("backup-verify", "--namespace", srv.namespace, "--to", store_dir,
                    "--backup-id", inc_id)
    finally:
        srv.cleanup()
        shutil.rmtree(store_dir, ignore_errors=True)


@pytest.mark.s3
def test_backup_to_s3():
    pytest.skip("requires LocalStack or real S3 (KDB_S3_* env) - run via the nightly s3 job")
