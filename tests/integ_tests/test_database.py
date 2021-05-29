import pytest
from photomanager import database


def test_database_create(tmpdir):
    db = database.Database()
    db.to_file(tmpdir / "test.json")
    db2 = db.from_file(tmpdir / "test.json")
    assert db.json == db2.json
    db.to_file(tmpdir / "test.json.gz")
    db2 = db.from_file(tmpdir / "test.json.gz")
    assert db.json == db2.json
    db.to_file(tmpdir / "test.json.zstd")
    db2 = db.from_file(tmpdir / "test.json.zstd")
    assert db.json == db2.json
