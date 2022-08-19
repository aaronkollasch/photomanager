import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import orjson
import pytest
import zstandard

from photomanager.database import Database, DatabaseException, sizeof_fmt
from photomanager.hasher import HashAlgorithm
from photomanager.photofile import NAME_MAP_ENC, PhotoFile

sizeof_fmt_expected_results = [
    (-1, "-1 bytes"),
    (0, "0 bytes"),
    (1, "1 byte"),
    (1023, "1023 bytes"),
    (1024, "1 kB"),
    (1024**2 - 1, "1024 kB"),
    (1024**2, "1.0 MB"),
    (1024**3 - 1, "1024.0 MB"),
    (1024**3, "1.00 GB"),
    (1024**3 * 5.34, "5.34 GB"),
    (1024**4 - 1, "1024.00 GB"),
    (1024**4, "1.00 TB"),
    (1024**5 - 10, "1024.00 TB"),
    (1024**5, "1.00 PB"),
]


@pytest.mark.parametrize("sizeof_fmt_tup", sizeof_fmt_expected_results)
def test_sizeof_fmt(sizeof_fmt_tup):
    assert sizeof_fmt(sizeof_fmt_tup[0]) == sizeof_fmt_tup[1]


def test_database_load_version_1():
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
            "priority": 20,
            "tz_offset": -14400
        }
    ]
},
"command_history": {
    "2021-03-08_23-56-00Z": "photomanager create --db test.json",
    "2021-03-08_23-57-00Z": "photomanager import --db test.json test.jpg"
}
}"""
    db = Database.from_json(json_data)
    print(db.db)
    assert db.version == Database.VERSION
    assert db.hash_algorithm == HashAlgorithm.SHA256
    assert db.db["timezone_default"] == "local"
    assert db.timezone_default is None
    photo_db_expected = {
        "d239210f00534b76a2b215e073f75832": [
            PhotoFile.from_dict(
                {
                    "chk": "deadbeef",
                    "src": "/a/b/c.jpg",
                    "dt": "2015:08:27 04:09:36.50",
                    "ts": 1440662976.5,
                    "fsz": 1024,
                    "sto": "/d/e/f.jpg",
                    "prio": 11,
                }
            ),
            PhotoFile.from_dict(
                {
                    "chk": "deadbeef",
                    "src": "/g/b/c.jpg",
                    "dt": "2015:08:27 04:09:36.50",
                    "ts": 1440662976.5,
                    "fsz": 1024,
                    "sto": "",
                    "prio": 20,
                    "tzo": -14400,
                }
            ),
        ]
    }
    command_history_expected = {
        "2021-03-08_23-56-00Z": "photomanager create --db test.json",
        "2021-03-08_23-57-00Z": "photomanager import --db test.json test.jpg",
    }
    db_expected = {
        "version": Database.VERSION,
        "hash_algorithm": HashAlgorithm.SHA256,
        "timezone_default": "local",
        "photo_db": photo_db_expected,
        "command_history": command_history_expected,
    }
    assert db.photo_db == photo_db_expected
    assert db.command_history == command_history_expected
    assert orjson.loads(db.json) != orjson.loads(json_data)
    assert db.db == db_expected
    assert db == Database.from_dict(orjson.loads(json_data))
    assert db.get_stats() == (1, 2, 1, 1024)


def test_database_init_update_version_1():
    """
    Database will upgrade loaded database files to current version
    """
    json_data = b"""{
  "version": 1,
  "hash_algorithm": "sha256",
  "timezone_default": "-0400",
  "photo_db": {
    "d239210f00534b76a2b215e073f75832": [
      {
        "checksum": "deadbeef",
        "source_path": "/a/b/c.jpg",
        "datetime": "2015:08:27 04:09:36.50",
        "timestamp": 1440662976.5,
        "file_size": 1024,
        "store_path": "/d/e/f.jpg",
        "priority": 11,
        "tz_offset": null
      },
      {
        "checksum": "deadbeef",
        "source_path": "/g/b/c.jpg",
        "datetime": "2015:08:27 04:09:36.50",
        "timestamp": 1440662976.5,
        "file_size": 1024,
        "store_path": "",
        "priority": 20,
        "tz_offset": -14400
      }
    ]
  },
  "command_history": {
    "2021-03-08_23-56-00Z": "photomanager create --db test.json",
    "2021-03-08_23-57-00Z": "photomanager import --db test.json test.jpg"
  }
}"""
    new_json_data = json_data.replace(
        b'"version": 1', f'"version": {Database.VERSION}'.encode()
    )
    for k, v in NAME_MAP_ENC.items():
        new_json_data = new_json_data.replace(
            b'"' + k.encode() + b'"',
            b'"' + v.encode() + b'"',
        )
    db = Database.from_json(json_data)
    print(db.db)
    assert db.db["timezone_default"] == "-0400"
    assert db.timezone_default == timezone(timedelta(days=-1, seconds=72000))
    assert orjson.loads(db.json) == orjson.loads(new_json_data)
    assert db.to_json(pretty=True) == new_json_data


def test_database_load_version_3():
    json_data = b"""{
"version": 3,
"hash_algorithm": "sha256",
"photo_db": {
    "QKEsTn2X": [
        {
            "chk": "deadbeef",
            "src": "/a/b/c.jpg",
            "dt": "2015:08:27 04:09:36.50",
            "ts": 1440662976.5,
            "fsz": 1024,
            "sto": "/d/e/f.jpg",
            "prio": 11,
            "tzo": null
        },
        {
            "chk": "deadbeef",
            "src": "/g/b/c.jpg",
            "dt": "2015:08:27 04:09:36.50",
            "ts": 1440662976.5,
            "fsz": 1024,
            "sto": "",
            "prio": 20,
            "tzo": -14400
        }
    ]
},
"command_history": {
    "2021-03-08_23-56-00Z": "photomanager create --db test.json",
    "2021-03-08_23-57-00Z": "photomanager import --db test.json test.jpg"
}
}""".replace(
        b"VERSION", f"{Database.VERSION}".encode()
    )
    db = Database.from_json(json_data)
    print(db.db)
    assert db.version == Database.VERSION
    assert db.hash_algorithm == HashAlgorithm.SHA256
    assert db.db["timezone_default"] == "local"
    assert db.timezone_default is None
    photo_db_expected = {
        "QKEsTn2X": [
            PhotoFile.from_dict(
                {
                    "chk": "deadbeef",
                    "src": "/a/b/c.jpg",
                    "dt": "2015:08:27 04:09:36.50",
                    "ts": 1440662976.5,
                    "fsz": 1024,
                    "sto": "/d/e/f.jpg",
                    "prio": 11,
                }
            ),
            PhotoFile.from_dict(
                {
                    "chk": "deadbeef",
                    "src": "/g/b/c.jpg",
                    "dt": "2015:08:27 04:09:36.50",
                    "ts": 1440662976.5,
                    "fsz": 1024,
                    "sto": "",
                    "prio": 20,
                    "tzo": -14400,
                }
            ),
        ]
    }
    command_history_expected = {
        "2021-03-08_23-56-00Z": "photomanager create --db test.json",
        "2021-03-08_23-57-00Z": "photomanager import --db test.json test.jpg",
    }
    db_expected = {
        "version": Database.VERSION,
        "hash_algorithm": HashAlgorithm.SHA256,
        "timezone_default": "local",
        "photo_db": photo_db_expected,
        "command_history": command_history_expected,
    }
    assert db.photo_db == photo_db_expected
    assert db.command_history == command_history_expected
    assert orjson.loads(db.json) != orjson.loads(json_data)
    assert db.db == db_expected
    assert db == Database.from_dict(orjson.loads(json_data))
    assert db.get_stats() == (1, 2, 1, 1024)


def test_database_init_version_too_high():
    """
    Database will raise DatabaseException if loaded database version is too high
    """
    json_data = b"""{
  "version": VERSION,
  "hash_algorithm": "sha256",
  "timezone_default": "-0400",
  "photo_db": {},
  "command_history": {}
}""".replace(
        b"VERSION", f"{Database.VERSION + 1}".encode()
    )
    with pytest.raises(DatabaseException):
        Database.from_json(json_data)


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


def test_database_save(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data)
    db.to_file(tmpdir / "test.json")
    db2 = db.from_file(tmpdir / "test.json")
    print(db.db, db2.db, sep="\n")
    assert db == db2
    db.to_file(tmpdir / "test.json.gz")
    db2 = db.from_file(tmpdir / "test.json.gz")
    print(db2.db)
    assert db == db2
    db.to_file(tmpdir / "test.json.zst")
    db2 = db.from_file(tmpdir / "test.json.zst")
    print(db2.db)
    assert db == db2


def test_database_load_zstd_checksum_error(tmpdir, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data)
    db.to_file(tmpdir / "test.json.zst")
    with open(tmpdir / "test.json.zst", "r+b") as f:
        f.seek(4)
        c = f.read(1)
        f.seek(4)
        f.write(bytes([ord(c) ^ 0b1]))
    with pytest.raises(zstandard.ZstdError):
        db.from_file(tmpdir / "test.json.zst")
    monkeypatch.setattr(
        zstandard,
        "decompress",
        lambda _: db.to_json(pretty=True).replace(c, bytes([ord(c) ^ 0b1])),
    )
    with pytest.raises(DatabaseException):
        db.from_file(tmpdir / "test.json.zst")


def test_database_overwrite_error(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data)
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


def test_database_add_photo_sort(caplog):
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data)
    uid = db.add_photo(
        PhotoFile(
            chk="deadbeef",
            src="/x/y/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="",
            prio=20,
        ),
        uid=None,
    )
    db.add_photo(
        PhotoFile(
            chk="deadbeef",
            src="/z/y/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="",
            prio=11,
        ),
        uid=None,
    )
    db.add_photo(
        PhotoFile(
            chk="deadbeef",
            src="/0/1/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="",
            prio=10,
        ),
        uid=None,
    )
    assert list(p.src for p in db.photo_db[uid]) == [
        "/0/1/c.jpg",
        "/a/b/c.jpg",
        "/z/y/c.jpg",
        "/x/y/c.jpg",
    ]


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
    """
    When there is no checksum match and an ambiguous timestamp+source match,
    find_photo returns the first match.
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data2)
    uid = db.find_photo(
        PhotoFile(
            chk="not_a_match",
            src="/x/y/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="",
            prio=10,
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
    """
    When adding a photo with a matching checksum for a different uid,
    the photo is not added and add_photo returns None.
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data2)
    uid = db.add_photo(
        PhotoFile(
            chk="deadbeef",
            src="/x/y/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="",
            prio=10,
        ),
        uid="uid2",
    )
    print([(r.levelname, r) for r in caplog.records])
    print(uid)
    assert uid is None


def test_database_add_photo_already_present(caplog):
    """
    When adding a photo that is already in the database,
    the photo is not added and add_photo returns None.
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data2)
    uid = db.add_photo(
        PhotoFile(
            chk="deadbeef",
            src="/a/b/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="",
            prio=10,
        ),
        uid="uid1",
    )
    print([(r.levelname, r) for r in caplog.records])
    print(uid)
    assert uid is None


def test_database_add_photo_same_source_new_checksum(caplog):
    """
    When adding a photo with a source_path in the database but a different checksum
    the photo is added to the database but a warning is issued.
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data2)
    uid = db.add_photo(
        PhotoFile(
            chk="not_a_match",
            src="/a/b/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="",
            prio=10,
        ),
        uid="uid1",
    )
    print([(r.levelname, r) for r in caplog.records])
    print(uid)
    assert uid == "uid1"
    assert db.hash_to_uid["not_a_match"] == "uid1"
    assert db.hash_to_uid["deadbeef"] == "uid1"
    print(db.photo_db["uid1"])
    assert len(db.photo_db["uid1"]) == 2
    print([(r.levelname, r) for r in caplog.records])
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert any(
        "Checksum of previously-indexed source photo has changed" in record.msg
        for record in caplog.records
    )


def test_database_clean_verify_absolute_subdir(tmpdir, caplog):
    """
    An exception is raised if subdir is an absolute path
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data2)
    with pytest.raises(DatabaseException):
        db.get_photos_to_remove(tmpdir / "a", subdirectory=tmpdir / "b")
    with pytest.raises(DatabaseException):
        db.get_stored_photos(subdirectory=tmpdir / "b")
    with pytest.raises(NotImplementedError):
        db.verify_indexed_photos()


def test_database_get_photos_to_collect_same_checksum_same_priority(caplog, tmpdir):
    """
    Photos with the same priority and checksum will not be recollected
    """
    caplog.set_level(logging.DEBUG)
    example_database = {
        "version": 1,
        "hash_algorithm": "sha256",
        "photo_db": {
            "uid1": [
                {
                    "checksum": "deadbeef",
                    "source_path": str(tmpdir / "source1" / "a.jpg"),
                    "datetime": "2015:08:27 04:09:36.50",
                    "timestamp": 1440662976.5,
                    "file_size": 1024,
                    "store_path": "a.jpg",
                    "priority": 11,
                },
                {
                    "checksum": "deadbeef",
                    "source_path": str(tmpdir / "source2" / "a.jpg"),
                    "datetime": "2015:08:27 04:09:36.50",
                    "timestamp": 1440662976.5,
                    "file_size": 1024,
                    "store_path": "",
                    "priority": 11,
                },
            ]
        },
        "command_history": {
            "2021-03-08_23-56-00Z": "photomanager create --db test.json"
        },
    }
    os.makedirs(tmpdir / "source1")
    os.makedirs(tmpdir / "source2")
    os.makedirs(tmpdir / "store")
    Path(tmpdir / "source1" / "a.jpg").touch()
    Path(tmpdir / "source2" / "a.jpg").touch()
    Path(tmpdir / "store" / "a.jpg").touch()
    db = Database.from_dict(example_database)
    (
        photos_to_copy,
        (num_copied_photos, num_added_photos, num_missed_photos, num_stored_photos),
    ) = db.get_photos_to_collect(tmpdir / "store")
    print(photos_to_copy)
    print(num_copied_photos, num_added_photos, num_missed_photos, num_stored_photos)
    assert len(photos_to_copy) == 0
    assert num_copied_photos == 0
    assert num_added_photos == 0
    assert num_missed_photos == 0
    assert num_stored_photos == 2


example_database_json_data3 = b"""{
"version": 2,
"hash_algorithm": "blake3",
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
        },
        {
            "checksum": "deedbeaf",
            "source_path": "/o/b/c.jpg",
            "datetime": "2015:08:27 04:09:36.50",
            "timestamp": 1440662976.5,
            "file_size": 1024,
            "store_path": "",
            "priority": 13
        }
    ],
    "uid2": [
        {
            "checksum": "aedfaedf",
            "source_path": "/a/c/e.jpg",
            "datetime": "2015:08:27 04:09:36.50",
            "timestamp": 1440662976.5,
            "file_size": 1024,
            "store_path": "/d/f/e.jpg",
            "priority": 11
        }
    ]
},
"command_history": {"2021-03-08_23-56-00Z": "photomanager create --db test.json"}
}"""


def test_database_list_sources(caplog):
    """
    The Database.sources property yields all src paths in the database
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data3)
    assert set(db.sources) == {
        "/a/b/c.jpg",
        "/o/b/c.jpg",
        "/a/c/e.jpg",
    }


def test_database_is_modified(caplog):
    """
    Database.is_modified() is True if Database.db has been modified
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data3)
    assert not db.is_modified()
    db.add_command("test")
    assert db.is_modified()
    db.reset_saved()
    assert not db.is_modified()
    db.photo_db["uid1"][1].sto = "/path/to/sto.jpg"
    assert db.is_modified()


def test_database_save_not_modified(tmpdir, caplog):
    """
    Database.save() will not save if the database is unchanged from loading
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data3)
    db_path = tmpdir / "photos.json"
    db.save(db_path, ["photomanager", "test"])
    assert "The database was not modified and will not be saved" in caplog.messages
    assert not db_path.exists()


def test_database_save_modified(tmpdir, caplog):
    """
    Database.save() will save if the database has been modified
    """
    caplog.set_level(logging.DEBUG)
    db = Database.from_json(example_database_json_data3)
    db.photo_db["uid1"][1].sto = "/path/to/sto.jpg"
    db_path = tmpdir / "photos.json"
    db.save(db_path, ["photomanager", "test"])
    assert "The database was not modified and will not be saved" not in caplog.messages
    assert db_path.exists()
    with open(db_path, "rb") as f:
        assert len(orjson.loads(f.read())["command_history"]) == 2
