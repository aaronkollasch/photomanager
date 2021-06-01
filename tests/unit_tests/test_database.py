import os
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import orjson
import pytest
import zstandard
from photomanager import database


def test_photofile_init():
    assert database.PhotoFile(
        checksum="deadbeef",
        source_path="/a/b/c.jpg",
        datetime="2015:08:27 04:09:36.50",
        timestamp=1440662976.5,
        file_size=1024,
        store_path="/d/e/f.jpg",
        priority=11,
    ).to_dict() == {
        "checksum": "deadbeef",
        "source_path": "/a/b/c.jpg",
        "datetime": "2015:08:27 04:09:36.50",
        "timestamp": 1440662976.5,
        "file_size": 1024,
        "store_path": "/d/e/f.jpg",
        "priority": 11,
    }


def test_photofile_eq():
    assert database.PhotoFile(
        checksum="deadbeef",
        source_path="/a/b/c.jpg",
        datetime="2015:08:27 04:09:36.50",
        timestamp=1440662976.5,
        file_size=1024,
        store_path="/d/e/f.jpg",
        priority=11,
    ) == database.PhotoFile(
        checksum="deadbeef",
        source_path="/a/b/c.jpg",
        datetime="2015:08:27 04:09:36.50",
        timestamp=1440662976.5,
        file_size=1024,
        store_path="/d/e/f.jpg",
        priority=11,
    )


def test_photofile_neq():
    pf1 = database.PhotoFile(
        checksum="deadbeef",
        source_path="/a/b/c.jpg",
        datetime="2015:08:27 04:09:36.50",
        timestamp=1440662976.5,
        file_size=1024,
        store_path="/d/e/f.jpg",
        priority=11,
    )
    assert pf1 != database.PhotoFile(
        checksum="deadfeed",
        source_path="/a/b/c.jpg",
        datetime="2015:08:27 04:09:36.50",
        timestamp=1440662976.5,
        file_size=1024,
        store_path="/d/e/f.jpg",
        priority=11,
    )
    assert pf1 != database.PhotoFile(
        checksum="deadbeef",
        source_path="/a/b/d.jpg",
        datetime="2015:08:27 04:09:36.50",
        timestamp=1440662976.5,
        file_size=1024,
        store_path="/d/e/f.jpg",
        priority=11,
    )
    assert pf1 != database.PhotoFile(
        checksum="deadbeef",
        source_path="/a/b/d.jpg",
        datetime="2015:08:27 04:09:36.50",
        timestamp=1440662976.0,
        file_size=1024,
        store_path="/d/e/f.jpg",
        priority=11,
    )


def test_sizeof_fmt():
    assert database.sizeof_fmt(-1) is None
    assert database.sizeof_fmt(0) == "0 bytes"
    assert database.sizeof_fmt(1) == "1 byte"
    assert database.sizeof_fmt(1023) == "1023 bytes"
    assert database.sizeof_fmt(1024) == "1 kB"
    assert database.sizeof_fmt(1024 ** 2 - 1) == "1024 kB"
    assert database.sizeof_fmt(1024 ** 2) == "1.0 MB"
    assert database.sizeof_fmt(1024 ** 3 - 1) == "1024.0 MB"
    assert database.sizeof_fmt(1024 ** 3) == "1.00 GB"
    assert database.sizeof_fmt(1024 ** 3 * 5.34) == "5.34 GB"
    assert database.sizeof_fmt(1024 ** 4 - 1) == "1024.00 GB"
    assert database.sizeof_fmt(1024 ** 4) == "1.00 TB"
    assert database.sizeof_fmt(1024 ** 5 - 10) == "1024.00 TB"
    assert database.sizeof_fmt(1024 ** 5) == "1.00 PB"


def test_database_init1():
    json_data = b"""{
"version": 1,
"hash_algorithm": "sha256",
"photo_db": {
    "d239210f00534b76a2b215e073f75832": [
        {
            "checksum": "deadbeef",
            "source_path": "/a/b/c.jpg",
            "datetime": "2015:08:27 04:09:36.50",
            "timestamp": 1440662976.5,
            "file_size": 1024,
            "store_path": "/d/e/f.jpg",
            "priority": 11
        },
        {
            "checksum": "deadbeef",
            "source_path": "/g/b/c.jpg",
            "datetime": "2015:08:27 04:09:36.50",
            "timestamp": 1440662976.5,
            "file_size": 1024,
            "store_path": "",
            "priority": 20
        }
    ]
},
"command_history": {"2021-03-08_23-56-00Z": "photomanager create --db test.json"}
}"""
    db = database.Database.from_json(json_data)
    print(db.db)
    assert db.version == database.Database.VERSION
    assert db.hash_algorithm == "sha256"
    assert db.db["timezone_default"] == "local"
    assert db.timezone_default is None
    photo_db_expected = {
        "d239210f00534b76a2b215e073f75832": [
            database.PhotoFile.from_dict(
                {
                    "checksum": "deadbeef",
                    "source_path": "/a/b/c.jpg",
                    "datetime": "2015:08:27 04:09:36.50",
                    "timestamp": 1440662976.5,
                    "file_size": 1024,
                    "store_path": "/d/e/f.jpg",
                    "priority": 11,
                }
            ),
            database.PhotoFile.from_dict(
                {
                    "checksum": "deadbeef",
                    "source_path": "/g/b/c.jpg",
                    "datetime": "2015:08:27 04:09:36.50",
                    "timestamp": 1440662976.5,
                    "file_size": 1024,
                    "store_path": "",
                    "priority": 20,
                }
            ),
        ]
    }
    command_history_expected = {
        "2021-03-08_23-56-00Z": "photomanager create --db test.json"
    }
    db_expected = {
        "version": database.Database.VERSION,
        "hash_algorithm": "sha256",
        "timezone_default": "local",
        "photo_db": photo_db_expected,
        "command_history": command_history_expected,
    }
    assert db.photo_db == photo_db_expected
    assert db.command_history == command_history_expected
    assert orjson.loads(db.json) != orjson.loads(json_data)
    assert db.db == db_expected
    assert db == database.Database.from_dict(orjson.loads(json_data))
    assert db.get_stats() == (1, 2, 1, 1024)


def test_database_init2():
    json_data = b"""{
"version": 1,
"hash_algorithm": "sha256",
"timezone_default": "-0400",
"photo_db": {},
"command_history": {}
}"""
    db = database.Database.from_json(json_data)
    print(db.db)
    assert db.db["timezone_default"] == "-0400"
    assert db.timezone_default == timezone(timedelta(days=-1, seconds=72000))


example_database_json_data = b"""{
"version": 1,
"hash_algorithm": "sha256",
"photo_db": {
    "d239210f00534b76a2b215e073f75832": [
        {
            "checksum": "deadbeef",
            "source_path": "/a/b/c.jpg",
            "datetime": "2015:08:27 04:09:36.50",
            "timestamp": 1440662976.5,
            "file_size": 1024,
            "store_path": "/d/e/f.jpg",
            "priority": 11
        }
    ]
},
"command_history": {"2021-03-08_23-56-00Z": "photomanager create --db test.json"}
}"""


def test_database_save(tmpdir):
    db = database.Database.from_json(example_database_json_data)
    db.to_file(tmpdir / "test.json")
    db2 = db.from_file(tmpdir / "test.json")
    assert db == db2
    db.to_file(tmpdir / "test.json.gz")
    db2 = db.from_file(tmpdir / "test.json.gz")
    assert db == db2
    db.to_file(tmpdir / "test.json.zst")
    db2 = db.from_file(tmpdir / "test.json.zst")
    assert db == db2


def test_database_load_zstd_checksum_error(tmpdir, monkeypatch):
    db = database.Database.from_json(example_database_json_data)
    db.to_file(tmpdir / "test.json.zst")
    with open(tmpdir / "test.json.zst", "r+b") as f:
        f.seek(4)
        c = f.read(1)
        f.seek(4)
        f.write(bytes([ord(c) ^ 0b1]))
    with pytest.raises(zstandard.ZstdError):
        db.from_file(tmpdir / "test.json.zst")
    monkeypatch.setattr(
        zstandard, "decompress", lambda _: db.json.replace(c, bytes([ord(c) ^ 0b1]))
    )
    with pytest.raises(database.DatabaseException):
        db.from_file(tmpdir / "test.json.zst")


def test_database_overwrite_error(tmpdir):
    db = database.Database.from_json(example_database_json_data)
    path = Path(tmpdir / "test.json")
    db.to_file(path)
    base_path = path
    for _ in path.suffixes:
        base_path = base_path.with_suffix("")
    timestamp_str = datetime.fromtimestamp(path.stat().st_mtime).strftime(
        "%Y-%m-%d_%H-%M-%S"
    )
    new_path = base_path.with_name(f"{base_path.name}_{timestamp_str}").with_suffix(
        "".join(path.suffixes)
    )
    os.makedirs(new_path)
    (new_path / "file.txt").touch()
    db.to_file(path)
    print(tmpdir.listdir())
    assert (tmpdir / "test_1.json").exists()

    Path(tmpdir / "test_0.json").touch()
    Path(tmpdir / "test_a.json").touch()
    db.to_file(path)
    print(tmpdir.listdir())
    assert (tmpdir / "test_2.json").exists()

    path = Path(tmpdir / "test_2.json")
    base_path = path
    for _ in path.suffixes:
        base_path = base_path.with_suffix("")
    timestamp_str = datetime.fromtimestamp(path.stat().st_mtime).strftime(
        "%Y-%m-%d_%H-%M-%S"
    )
    new_path = base_path.with_name(f"{base_path.name}_{timestamp_str}").with_suffix(
        "".join(path.suffixes)
    )
    os.makedirs(new_path)
    (new_path / "file.txt").touch()
    db.to_file(path)
    print(tmpdir.listdir())
    assert (tmpdir / "test_3.json").exists()


example_database_json_data2 = b"""{
"version": 1,
"hash_algorithm": "sha256",
"photo_db": {
    "uid1": [
        {
            "checksum": "deadbeef",
            "source_path": "/a/b/c.jpg",
            "datetime": "2015:08:27 04:09:36.50",
            "timestamp": 1440662976.5,
            "file_size": 1024,
            "store_path": "/d/e/c.jpg",
            "priority": 11
        }
    ],
    "uid2": [
        {
            "checksum": "aedfaedf",
            "source_path": "/a/c/c.jpg",
            "datetime": "2015:08:27 04:09:36.50",
            "timestamp": 1440662976.5,
            "file_size": 1024,
            "store_path": "/d/f/c.jpg",
            "priority": 11
        }
    ]
},
"command_history": {"2021-03-08_23-56-00Z": "photomanager create --db test.json"}
}"""


def test_database_find_photo_ambiguous(caplog):
    caplog.set_level(logging.DEBUG)
    db = database.Database.from_json(example_database_json_data2)
    uid = db.find_photo(
        database.PhotoFile(
            checksum="not_a_match",
            source_path="/x/y/c.jpg",
            datetime="2015:08:27 04:09:36.50",
            timestamp=1440662976.5,
            file_size=1024,
            store_path="",
            priority=10,
        )
    )
    print([(r.levelname, r) for r in caplog.records])
    print(uid)
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert any(
        "ambiguous timestamp+name match" in record.msg for record in caplog.records
    )
    assert uid == "uid1"


def test_database_add_photo_wrong_uid(caplog):
    caplog.set_level(logging.DEBUG)
    db = database.Database.from_json(example_database_json_data2)
    uid = db.add_photo(
        database.PhotoFile(
            checksum="deadbeef",
            source_path="/x/y/c.jpg",
            datetime="2015:08:27 04:09:36.50",
            timestamp=1440662976.5,
            file_size=1024,
            store_path="",
            priority=10,
        ),
        uid="uid2",
    )
    print([(r.levelname, r) for r in caplog.records])
    print(uid)
    assert uid is None


def test_database_add_photo_already_present(caplog):
    caplog.set_level(logging.DEBUG)
    db = database.Database.from_json(example_database_json_data2)
    uid = db.add_photo(
        database.PhotoFile(
            checksum="deadbeef",
            source_path="/a/b/c.jpg",
            datetime="2015:08:27 04:09:36.50",
            timestamp=1440662976.5,
            file_size=1024,
            store_path="",
            priority=10,
        ),
        uid="uid1",
    )
    print([(r.levelname, r) for r in caplog.records])
    print(uid)
    assert uid is None


def test_database_add_photo_same_source_new_checksum(caplog):
    caplog.set_level(logging.DEBUG)
    db = database.Database.from_json(example_database_json_data2)
    uid = db.add_photo(
        database.PhotoFile(
            checksum="not_a_match",
            source_path="/a/b/c.jpg",
            datetime="2015:08:27 04:09:36.50",
            timestamp=1440662976.5,
            file_size=1024,
            store_path="",
            priority=10,
        ),
        uid="uid1",
    )
    print([(r.levelname, r) for r in caplog.records])
    print(uid)
    assert uid == "uid1"
    assert db.hash_to_uid["not_a_match"] == "uid1"
    print(db.photo_db["uid1"])
    print([(r.levelname, r) for r in caplog.records])
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert any(
        "Adding already stored photo with new checksum" in record.msg
        for record in caplog.records
    )


def test_database_clean_verify_absolute_subdir(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    db = database.Database.from_json(example_database_json_data2)
    with pytest.raises(database.DatabaseException):
        db.clean_stored_photos(tmpdir / "a", subdirectory=tmpdir / "b")
    with pytest.raises(database.DatabaseException):
        db.verify_stored_photos(tmpdir / "a", subdirectory=tmpdir / "b")
    with pytest.raises(NotImplementedError):
        db.verify_indexed_photos()
