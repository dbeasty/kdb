"""The media generator itself (media_fixtures.py): no server. The attachment tests trust these
properties - deterministic bytes, edits confined to known ranges, truth that catches damage."""

from __future__ import annotations

import pytest

import media_fixtures as mf


def test_generators_are_deterministic(tmp_path):
    for kind, make in [("y4m", lambda p: mf.make_y4m(p, 5, seed=3)),
                       ("wav", lambda p: mf.make_wav(p, 0.5, seed=3)),
                       ("png", lambda p: mf.make_png(p, seed=3)),
                       ("bin", lambda p: mf.make_bin(p, 300_000, seed=3))]:
        a, b = make(tmp_path / f"a.{kind}"), make(tmp_path / f"b.{kind}")
        assert a == b, kind
        assert mf.truth_of(kind, tmp_path / f"a.{kind}") == a, kind


def test_y4m_truth_matches_layout_and_replace_touches_only_those_frames(tmp_path):
    t = mf.make_y4m(tmp_path / "v1.y4m", 10)
    fb = mf.y4m_frame_bytes(320, 240)
    assert t.frames == 10 and t.size == mf.y4m_header_len() + 10 * (6 + fb)
    t2 = mf.replace_frames(tmp_path / "v2.y4m", 10, range(4, 6))
    assert t2.size == t.size
    changed = [i for i in range(10) if t.frame_hashes[i] != t2.frame_hashes[i]]
    assert changed == [4, 5]
    a, b = (tmp_path / "v1.y4m").read_bytes(), (tmp_path / "v2.y4m").read_bytes()
    diff = [i for i in range(len(a)) if a[i] != b[i]]
    lo = mf.y4m_header_len() + 4 * (6 + fb)
    assert diff and lo <= diff[0] and diff[-1] < lo + 2 * (6 + fb)


def test_truth_detects_damage(tmp_path):
    p = tmp_path / "v.y4m"
    t = mf.make_y4m(p, 4)
    raw = bytearray(p.read_bytes())
    raw[len(raw) // 2] ^= 0xFF
    p.write_bytes(bytes(raw))
    assert not mf.y4m_truth(p).same_media(t)
    p.write_bytes(bytes(raw[:-10]))
    with pytest.raises(ValueError):
        mf.y4m_truth(p)


def test_wav_retitle_changes_header_only(tmp_path):
    a = mf.make_wav(tmp_path / "a.wav", 1.0, title="first")
    b = mf.make_wav(tmp_path / "b.wav", 1.0, title="second")
    assert (a.title, b.title) == ("first", "second")
    assert a.frame_hashes == b.frame_hashes and a.sha256 != b.sha256


def test_insert_bytes_shifts_the_tail(tmp_path):
    mf.make_bin(tmp_path / "a.bin", 100_000)
    t = mf.insert_bytes(tmp_path / "a.bin", tmp_path / "b.bin", 5000, b"xyz")
    a, b = (tmp_path / "a.bin").read_bytes(), (tmp_path / "b.bin").read_bytes()
    assert t.size == 100_003 and b[:5000] == a[:5000] and b[5003:] == a[5000:]


def test_mp4_decodes_frame_by_frame(tmp_path):
    t = mf.make_mp4(tmp_path / "a.mp4", seconds=1)
    assert t.frames == 25 and len(set(t.frame_hashes)) > 1
    raw = bytearray((tmp_path / "a.mp4").read_bytes())
    raw[len(raw) // 2:len(raw) // 2 + 4096] = bytes(4096)
    (tmp_path / "b.mp4").write_bytes(bytes(raw))
    try:
        damaged = mf.mp4_truth(tmp_path / "b.mp4")
    except ValueError:
        return  # refused to decode: detected
    assert not damaged.same_media(t)
