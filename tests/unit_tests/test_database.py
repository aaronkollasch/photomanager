import pytest
import orjson
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


def test_database_init():
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
        }
    ]
},
"command_history": {"2021-03-08_23-56-00Z": "photomanager create --db test.json"}
}"""
    db = database.Database.from_json(json_data)
    assert db.version == 1
    assert db.hash_algorithm == "sha256"
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
            )
        ]
    }
    command_history_expected = {
        "2021-03-08_23-56-00Z": "photomanager create --db test.json"
    }
    db_expected = {
        "version": 1,
        "hash_algorithm": "sha256",
        "photo_db": photo_db_expected,
        "command_history": command_history_expected,
    }
    assert db.photo_db == photo_db_expected
    assert db.command_history == command_history_expected
    assert orjson.loads(db.json) == orjson.loads(json_data)
    assert db.db == db_expected
