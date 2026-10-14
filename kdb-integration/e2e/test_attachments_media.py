"""Attachments end to end with real media (docs/kdb-attachments-e2e-test-plan.md, series B/V/Q).

Every file is generated (media_fixtures.py), attached through the Go client or SQL over the
network, read back by streaming, and checked twice: byte for byte (SHA-256) and as media (each
frame re-parsed or decoded and compared with the hashes recorded when it was generated).
"""

from __future__ import annotations

import pytest

import media_fixtures as mf
from attachment_fixtures import (
    attach, attachment_dir, doc_id, list_attachments, maintained_server, read_attachment,
    sql_attach, stored_bytes, table, tx_attach, wait_stored_settles)
from cluster_fixtures import documents, head
from server_fixtures import KdbServer, sql_exec, sql_query, upsert

pytestmark = [pytest.mark.server]

KiB, MiB = 1024, 1024 * 1024
SMALL_MAX = 256 * KiB   # attachstore SmallMaxBytes: at or below, inline in the pointer segment
CHUNK = MiB             # scheme 1 chunk


def check_read(server: KdbServer, doc: str, name: str, truth: mf.MediaTruth, tmp_path, as_of: str = "",
               tag: str = "") -> dict:
    out = tmp_path / f"read-{doc[-6:]}-{name}{tag}"
    got = read_attachment(server, doc, name, as_of=as_of, out=out)
    assert "error" not in got, f"{name}: {got}"
    assert (got["sha256"], got["size"]) == (truth.sha256, truth.size), f"{name}: bytes differ: {got}"
    again = mf.truth_of(truth.kind, out)
    assert again.same_media(truth), f"{name}: downloaded file no longer parses as the same {truth.kind}"
    return got


# -- B: basics -------------------------------------------------------------------------------

def test_round_trip_every_size_class_and_format_survives_restart(tmp_path):
    """B1 + B6: sizes on both sides of the inline threshold and the chunk size, and every media
    format, written and read over the network, then again after a restart."""
    cases: list[tuple[str, mf.MediaTruth]] = []
    for size in [0, 1, 100 * KiB, SMALL_MAX - 1, SMALL_MAX, SMALL_MAX + 1, CHUNK - 1, CHUNK, CHUNK + 1,
                 3 * CHUNK + 777]:
        p = tmp_path / f"b{size}.bin"
        cases.append((str(p), mf.make_bin(p, size, seed=size + 1)))
    media = [("v.y4m", lambda p: mf.make_y4m(p, 40)), ("a.wav", lambda p: mf.make_wav(p, 2.0)),
             ("i.png", lambda p: mf.make_png(p)), ("m.mp4", lambda p: mf.make_mp4(p, seconds=2))]
    for name, make in media:
        p = tmp_path / name
        cases.append((str(p), make(p)))

    srv = KdbServer(control=True).start()
    try:
        names = []
        for i, (path, truth) in enumerate(cases):
            doc = doc_id(0xB100 + i)
            upsert(srv, doc, {"id": doc, "i": i})
            name = path.rsplit("/", 1)[-1]
            info = attach(srv, doc, name, path, mime=f"test/{truth.kind}")
            assert info["size"] == truth.size, (name, info)
            names.append((doc, name, truth))
        for doc, name, truth in names:
            check_read(srv, doc, name, truth, tmp_path)
            [item] = list_attachments(srv, doc)["attachments"]
            assert (item["name"], item["size"], item["mime"]) == (name, truth.size, f"test/{truth.kind}")
        srv.stop()
        srv.restart()
        for doc, name, truth in names:
            check_read(srv, doc, name, truth, tmp_path, tag=".after-restart")
    finally:
        srv.stop()


def test_document_and_attachment_are_one_commit(tmp_path):
    """B2: the JSON write and the attachment land in one commit. As of the commit before it,
    neither exists; as of it, both do; the document's JSON never mentions the attachment."""
    srv = KdbServer(control=True).start()
    try:
        upsert(srv, doc_id(1), {"id": doc_id(1), "kind": "other"})
        before = head(srv)
        p = tmp_path / "clip.y4m"
        truth = mf.make_y4m(p, 12)
        doc = doc_id(0xB200)
        res = tx_attach(srv, doc, {"id": doc, "title": "clip", "codec": "y4m"}, "clip.y4m", p,
                        mime="video/x-yuv4mpeg")
        assert head(srv) == res["commit"]

        assert list_attachments(srv, doc, as_of=before).get("attachments") in (None, [])
        at = check_read(srv, doc, "clip.y4m", truth, tmp_path, as_of=res["commit"])
        assert at["commit"] == res["commit"]
        rows = sql_query(srv, f"SELECT title, ATTACHMENT_COUNT(), ATTACHMENT_SIZE('clip.y4m') FROM {table(srv)} "
                              f"WHERE codec = 'y4m'")
        assert rows == [["clip", "1", str(truth.size)]], rows
        body = sql_query(srv, f"SELECT _doc FROM {table(srv)} WHERE codec = 'y4m'")
        assert "clip.y4m" not in body[0][0] and res["hash"] not in body[0][0], body
    finally:
        srv.stop()


def test_identical_bytes_are_stored_once(tmp_path):
    """B3: content addressing - the same bytes attached to a second document cost no new segment
    bytes, only a pointer."""
    srv = KdbServer().start()
    try:
        p = tmp_path / "same.bin"
        truth = mf.make_bin(p, 3 * CHUNK + 5)
        for n in (1, 2):
            upsert(srv, doc_id(0xB300 + n), {"id": doc_id(0xB300 + n)})
        attach(srv, doc_id(0xB301), "a", p)
        after_first = stored_bytes(srv)
        assert after_first >= truth.size
        attach(srv, doc_id(0xB302), "b", p)
        grew = stored_bytes(srv) - after_first
        assert grew < 64 * KiB, f"a second copy of {truth.size} bytes grew the store by {grew}"
        for n, name in ((1, "a"), (2, "b")):
            check_read(srv, doc_id(0xB300 + n), name, truth, tmp_path)
    finally:
        srv.stop()


def test_corrupted_bytes_are_refused_never_served(tmp_path):
    """B7: a byte flipped on disk in a stored chunk makes the read fail as corrupt - it never
    returns wrong bytes."""
    srv = KdbServer().start()
    try:
        p = tmp_path / "v.y4m"
        mf.make_y4m(p, 40)
        doc = doc_id(0xB700)
        upsert(srv, doc, {"id": doc})
        attach(srv, doc, "v.y4m", p)
        srv.stop()
        segs = sorted(attachment_dir(srv).glob("u-*"), key=lambda f: f.stat().st_size)
        target = segs[-1]  # the bytes segment: the largest
        raw = bytearray(target.read_bytes())
        raw[len(raw) // 2] ^= 0x5A
        target.write_bytes(bytes(raw))
        srv.restart()
        got = read_attachment(srv, doc, "v.y4m")
        assert got.get("kind") == "corrupt", f"a corrupted attachment was not refused as corrupt: {got}"
    finally:
        srv.stop()


def test_dropped_upload_resumes_and_sends_only_the_rest(tmp_path):
    """B9: an upload that loses its source part way resumes from what the server holds."""
    srv = KdbServer().start()
    try:
        p = tmp_path / "big.bin"
        truth = mf.make_bin(p, 6 * CHUNK + 123)
        doc = doc_id(0xB900)
        upsert(srv, doc, {"id": doc})
        info = attach(srv, doc, "big.bin", p, drop_at=2 * CHUNK + CHUNK // 2)
        assert info["resumed"] and info["size"] == truth.size, info
        check_read(srv, doc, "big.bin", truth, tmp_path)
    finally:
        srv.stop()


def test_several_named_attachments_per_record(tmp_path):
    """A record holds several attachments; each is replaced, renamed or dropped alone (here
    through SQL), and deleting the record removes all of them."""
    srv = KdbServer().start()
    try:
        doc = doc_id(0xBA00)
        upsert(srv, doc, {"id": doc, "title": "shoot"})
        files = {"cover.png": mf.make_png(tmp_path / "cover.png"),
                 "take1.y4m": mf.make_y4m(tmp_path / "take1.y4m", 8),
                 "sound.wav": mf.make_wav(tmp_path / "sound.wav", 1.0)}
        ids = {name: attach(srv, doc, name, tmp_path / name)["id"] for name in files}
        assert sorted(a["name"] for a in list_attachments(srv, doc)["attachments"]) == sorted(files)

        take2 = mf.make_y4m(tmp_path / "take2.y4m", 8, seed=2)
        attach(srv, doc, "take1.y4m", tmp_path / "take2.y4m")  # replace by name, same ID
        t = table(srv)
        sql_exec(srv, f"UPDATE {t} RENAME ATTACHMENT 'take1.y4m' TO 'final.y4m', "
                      f"DROP ATTACHMENT 'sound.wav' WHERE title = 'shoot'")
        listing = {a["name"]: a for a in list_attachments(srv, doc)["attachments"]}
        assert sorted(listing) == ["cover.png", "final.y4m"], listing
        assert listing["final.y4m"]["id"] == ids["take1.y4m"]
        check_read(srv, doc, "final.y4m", take2, tmp_path)
        check_read(srv, doc, "cover.png", files["cover.png"], tmp_path)
        assert sql_query(srv, f"SELECT ATTACHMENT_COUNT() FROM {t} WHERE title = 'shoot'") == [["2"]]

        sql_exec(srv, f"DELETE FROM {t} WHERE title = 'shoot'")
        assert read_attachment(srv, doc, "cover.png").get("kind") == "not_found"
    finally:
        srv.stop()


# -- V: versions and delta storage ------------------------------------------------------------

def test_every_version_reads_back_as_of_its_commit(tmp_path):
    """V1 + V5: one attachment through many edits; as of each commit the bytes (and the frames)
    are exactly that version's, and the listing as of the commit shows that version's size."""
    srv = KdbServer().start()
    try:
        doc = doc_id(0xC100)
        upsert(srv, doc, {"id": doc})
        edits = [lambda p: mf.make_y4m(p, 20),
                 lambda p: mf.replace_frames(p, 20, range(3, 5)),
                 lambda p: mf.make_y4m(p, 26),                        # append
                 lambda p: mf.make_y4m(p, 14),                        # truncate
                 lambda p: mf.replace_frames(p, 14, range(0, 1), new_seed=7),
                 lambda p: mf.make_y4m(p, 14, seed=5)]                # everything changes
        versions = []
        for i, make in enumerate(edits):
            p = tmp_path / f"v{i}.y4m"
            truth = make(p)
            versions.append((attach(srv, doc, "clip.y4m", p)["commit"], truth))
        for i, (commit, truth) in enumerate(versions):
            check_read(srv, doc, "clip.y4m", truth, tmp_path, as_of=commit, tag=f".v{i}")
            [item] = list_attachments(srv, doc, as_of=commit)["attachments"]
            assert item["size"] == truth.size
        check_read(srv, doc, "clip.y4m", versions[-1][1], tmp_path, tag=".head")
    finally:
        srv.stop()


@pytest.mark.parametrize("edit", ["replace_frames", "wav_retitle", "insert_bytes"])
def test_a_small_edit_stores_only_a_delta(tmp_path, edit):
    """V2/V4: after a small edit, maintenance re-stores the replaced version as a delta against
    the new one, so the store holds about one copy plus the change - and both versions still read
    back exactly."""
    if edit == "replace_frames":
        v1 = mf.make_y4m(tmp_path / "v1", 60)
        v2 = mf.replace_frames(tmp_path / "v2", 60, range(30, 32))
        change = 2 * (mf.y4m_frame_bytes(320, 240) + 6)
    elif edit == "wav_retitle":
        v1 = mf.make_wav(tmp_path / "v1", 30.0, title="draft")
        v2 = mf.make_wav(tmp_path / "v2", 30.0, title="final master")
        change = 64
    else:
        v1 = mf.make_bin(tmp_path / "v1", 8 * CHUNK)
        v2 = mf.insert_bytes(tmp_path / "v1", tmp_path / "v2", 3 * CHUNK + 4321, b"inserted!" * 50)
        change = 450
    srv = maintained_server().start()
    try:
        doc = doc_id(0xC200)
        upsert(srv, doc, {"id": doc})
        c1 = attach(srv, doc, "f", tmp_path / "v1")["commit"]
        c2 = attach(srv, doc, "f", tmp_path / "v2")["commit"]
        whole = v1.size + v2.size
        # The delta encoder indexes the base in blocks; allow a block either side of the change
        # plus pointer/leaves overhead.
        budget = v2.size + change + 2 * 64 * KiB + 32 * KiB
        settled = wait_stored_settles(srv, below=budget, timeout=45)
        assert settled < whole, (settled, whole)
        check_read(srv, doc, "f", v1, tmp_path, as_of=c1, tag=".v1")
        check_read(srv, doc, "f", v2, tmp_path, as_of=c2, tag=".v2")
    finally:
        srv.stop()


def test_a_version_over_the_pass_budget_is_diffed_over_many_passes(tmp_path):
    """V2 at scale: a pass may read only 1 MiB diffing (--attachment-delta-step-mb 1), so a
    24 MiB version with an insertion - every chunk after it shifted - is diffed over dozens of
    maintenance passes, as a multi-GB one is under the default budget. The store settles at about
    one copy plus the change, and both versions read back exactly throughout and after."""
    v1 = mf.make_bin(tmp_path / "v1", 24 * CHUNK)
    v2 = mf.insert_bytes(tmp_path / "v1", tmp_path / "v2", 2 * CHUNK + 999, b"inserted!" * 50)
    srv = maintained_server(extra_args=["--attachment-delta-step-mb", "1"]).start()
    try:
        doc = doc_id(0xC250)
        upsert(srv, doc, {"id": doc})
        c1 = attach(srv, doc, "f", tmp_path / "v1")["commit"]
        c2 = attach(srv, doc, "f", tmp_path / "v2")["commit"]
        check_read(srv, doc, "f", v1, tmp_path, as_of=c1, tag=".mid")
        budget = v2.size + 450 + 256 * KiB
        settled = wait_stored_settles(srv, below=budget, timeout=90)
        assert settled < v1.size + v2.size, (settled, v1.size, v2.size)
        check_read(srv, doc, "f", v1, tmp_path, as_of=c1, tag=".v1")
        check_read(srv, doc, "f", v2, tmp_path, as_of=c2, tag=".v2")
    finally:
        srv.stop()


def test_with_deltas_off_every_version_stays_the_original_file(tmp_path):
    """--attachment-deltas=false: a small edit that would be a delta is not one - maintenance
    keeps both versions whole, and both read back exactly."""
    v1 = mf.make_bin(tmp_path / "v1", 8 * CHUNK)
    v2 = mf.insert_bytes(tmp_path / "v1", tmp_path / "v2", 3 * CHUNK + 4321, b"inserted!" * 50)
    srv = maintained_server(extra_args=["--attachment-deltas=false"]).start()
    try:
        doc = doc_id(0xC260)
        upsert(srv, doc, {"id": doc})
        c1 = attach(srv, doc, "f", tmp_path / "v1")["commit"]
        c2 = attach(srv, doc, "f", tmp_path / "v2")["commit"]
        settled = wait_stored_settles(srv, timeout=30, quiet=3.0)
        assert settled >= v1.size + v2.size, (settled, v1.size, v2.size)
        check_read(srv, doc, "f", v1, tmp_path, as_of=c1, tag=".v1")
        check_read(srv, doc, "f", v2, tmp_path, as_of=c2, tag=".v2")
    finally:
        srv.stop()


def test_reencoded_video_is_stored_whole_negative_control(tmp_path):
    """V3: re-encoding changes nearly every byte, so no delta is worth keeping: the store holds
    both versions about whole, and both still decode frame by frame."""
    v1 = mf.make_mp4(tmp_path / "v1.mp4", seconds=4)
    v2 = mf.reencode_mp4(tmp_path / "v1.mp4", tmp_path / "v2.mp4", crf=35)
    srv = maintained_server().start()
    try:
        doc = doc_id(0xC300)
        upsert(srv, doc, {"id": doc})
        c1 = attach(srv, doc, "m.mp4", tmp_path / "v1.mp4")["commit"]
        c2 = attach(srv, doc, "m.mp4", tmp_path / "v2.mp4")["commit"]
        settled = wait_stored_settles(srv, timeout=30, quiet=3.0)
        assert settled >= 0.9 * (v1.size + v2.size), (settled, v1.size, v2.size)
        check_read(srv, doc, "m.mp4", v1, tmp_path, as_of=c1, tag=".v1")
        check_read(srv, doc, "m.mp4", v2, tmp_path, as_of=c2, tag=".v2")
    finally:
        srv.stop()


def test_json_only_update_leaves_the_attachment_alone(tmp_path):
    """V6: changing the record's JSON neither touches the attachment's bytes nor its identity."""
    srv = KdbServer().start()
    try:
        doc = doc_id(0xC600)
        upsert(srv, doc, {"id": doc, "tags": ["a"]})
        truth = mf.make_y4m(tmp_path / "v.y4m", 20)
        h = attach(srv, doc, "v.y4m", tmp_path / "v.y4m")["hash"]
        before = stored_bytes(srv)
        for i in range(5):
            upsert(srv, doc, {"id": doc, "tags": ["a", f"t{i}"], "views": i})
        assert stored_bytes(srv) == before
        got = check_read(srv, doc, "v.y4m", truth, tmp_path)
        assert got["hash"] == h
    finally:
        srv.stop()


# -- Q: searchable metadata -------------------------------------------------------------------

def test_metadata_query_finds_the_right_files_and_their_bytes(tmp_path):
    """Q1 + Q2: a mixed catalog where every record and its media went in as one commit; SQL over
    the metadata returns exactly the records a Python-side filter does, and each hit's attachment
    is the file that was attached to it."""
    srv = KdbServer().start()
    try:
        makers = {"y4m": lambda p, s: mf.make_y4m(p, 4 + s % 5, seed=s),
                  "wav": lambda p, s: mf.make_wav(p, 0.5 + (s % 4) * 0.5, seed=s),
                  "png": lambda p, s: mf.make_png(p, seed=s)}
        catalog = {}
        for i in range(30):
            kind = ["y4m", "wav", "png"][i % 3]
            p = tmp_path / f"c{i}.{kind}"
            truth = makers[kind](p, i)
            meta = {"key": f"m{i:02d}", "kind": kind, "owner": ["ana", "bo", "cy"][i % 3 if i % 2 else 0],
                    "durationMs": (truth.frames * 40 if kind == "y4m" else truth.frames * 1000 // 22050
                                   if kind == "wav" else 0), "year": 2020 + i % 6}
            doc = doc_id(0xD000 + i)
            tx_attach(srv, doc, {"id": doc, **meta}, "media", p, mime=f"test/{kind}")
            catalog[meta["key"]] = (doc, meta, truth)

        t = table(srv)
        rows = sql_query(srv, f"SELECT key, ATTACHMENT_SIZE('media'), ATTACHMENT_MIME('media') FROM {t} "
                              f"WHERE kind <> 'png' AND durationMs >= 1000 AND year >= 2022")
        want = {k for k, (_, m, _) in catalog.items()
                if m["kind"] != "png" and m["durationMs"] >= 1000 and m["year"] >= 2022}
        assert want, "the filter should match something"
        assert {r[0] for r in rows} == want, (sorted(r[0] for r in rows), sorted(want))
        for key, size, mime in rows:
            doc, meta, truth = catalog[key]
            assert (int(size), mime) == (truth.size, f"test/{meta['kind']}")
            check_read(srv, doc, "media", truth, tmp_path)
        assert sql_query(srv, f"SELECT COUNT(*) FROM {t} WHERE ATTACHMENT_COUNT() = 1") == [["30"]]
    finally:
        srv.stop()


def test_sql_streams_large_media_in_and_the_api_streams_it_out(tmp_path):
    """INSERT ... WITH ATTACHMENT 'n' = STREAM ? with media bigger than a wire frame (16 MB) and
    than ATTACHMENT()'s inline cap; UPDATE ... SET ATTACHMENT replaces it the same way."""
    big = mf.make_y4m(tmp_path / "big.y4m", 160)   # ~18 MiB
    assert big.size > 16 * MiB
    clip = mf.make_mp4(tmp_path / "clip.mp4", seconds=3)
    srv = KdbServer(control=True).start()
    try:
        t = table(srv)
        res = sql_attach(srv, f"INSERT INTO {t} (key, kind) VALUES ('raw', 'y4m') "
                              f"WITH ATTACHMENT 'video' = STREAM ? MIME 'video/x-yuv4mpeg'", tmp_path / "big.y4m",
                         timeout=300)
        assert res["rows"] == 1 and res["size"] == big.size, res
        [doc] = list(documents(srv))  # SQL has no id column; the namespace holds just this record
        doc = doc.replace("-", "")
        check_read(srv, doc, "video", big, tmp_path)
        sql_attach(srv, f"UPDATE {t} SET ATTACHMENT 'video' = STREAM ? MIME 'video/mp4' WHERE key = 'raw'",
                   tmp_path / "clip.mp4")
        check_read(srv, doc, "video", clip, tmp_path, tag=".mp4")
        assert sql_query(srv, f"SELECT ATTACHMENT_MIME('video') FROM {t} WHERE key = 'raw'") == [["video/mp4"]]
    finally:
        srv.stop()
