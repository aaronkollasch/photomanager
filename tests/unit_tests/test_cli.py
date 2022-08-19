import json
import logging
import os
import sys
from pathlib import Path
from typing import cast

import pytest
from click import Group
from click.testing import CliRunner

from photomanager import cli, database, version


def check_dir_empty(dir_path):
    cwd_files = list(Path(dir_path).glob("*"))
    print(cwd_files)
    assert len(cwd_files) == 0


def test_cli_main(monkeypatch, capsys):
    from photomanager import __main__

    monkeypatch.setattr(__main__, "__name__", "__main__")
    monkeypatch.setattr(sys, "argv", ["photomanager", "--version"])
    with pytest.raises(SystemExit) as exit_type:
        __main__._init()
    captured = capsys.readouterr()
    assert exit_type.value.code == 0
    assert captured.out.strip() == f"photomanager {version}"
    assert captured.err == ""


def test_cli_exit_codes(monkeypatch):
    monkeypatch.setattr(cli, "__name__", "__main__")
    monkeypatch.setattr(sys, "argv", ["photomanager", "index"])
    with pytest.raises(SystemExit) as exit_type:
        cli._init()
    assert exit_type.value.code == 1


def test_cli_exit_code_no_files(tmpdir):
    assert cli._index(["--db", str(tmpdir / "none.json")], standalone_mode=False) == 1


def test_cli_create(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        result = runner.invoke(
            cast(Group, cli.main), ["create", "--db", str(tmpdir / "test.json")]
        )
        print(result.output)
        assert result.exit_code == 0
        with open(Path(tmpdir) / "test.json") as f:
            s = f.read()
        print(s)
        d = json.loads(s)
        assert d["photo_db"] == {}
        assert d["version"] == database.Database.VERSION
        assert d["hash_algorithm"] == database.DEFAULT_HASH_ALGO.value
        check_dir_empty(fs)


def test_cli_create_wrong_algorithm(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "create",
                "--db",
                str(tmpdir / "test.json"),
                "--hash-algorithm",
                "nonexistent-asdf",
            ],
        )
        print("\nCREATE invalid hash algorithm")
        print(result.output)
        print(result)
        assert result.exit_code == 2
        assert "Invalid value" in result.output
        check_dir_empty(fs)


def test_cli_index_directory_db(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(tmpdir),
            ],
        )
        print("\nINDEX directory db")
        print(result.output)
        print(result)
        assert result.exit_code == 2
        assert "is a directory" in result.output
        check_dir_empty(fs)


def test_cli_index_nothing(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(tmpdir / "test1.json"),
                "--priority",
                "10",
            ],
        )
        print("\nINDEX nothing")
        print(result.output)
        print(result)
        assert result.exit_code == 1
        assert "Nothing to index" in result.output
        check_dir_empty(fs)


def test_cli_create_collect_no_db(tmpdir, caplog):
    """
    If create and is run with no db argument, db is created at DEFAULT_DB
    """
    caplog.set_level(logging.DEBUG)
    CliRunner.isolated_filesystem(tmpdir)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        with open(tmpdir / "test.jpg", "wb") as f:
            f.write(b"contents")

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "create",
            ],
        )
        print("\nINDEX no-db")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert (Path(fs) / cli.DEFAULT_DB).is_file()

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "collect",
                "--destination",
                str(tmpdir / "dest"),
            ],
        )
        print("\nCOLLECT no-db")
        print(result.output)
        print(result)
        assert result.exit_code == 0


def test_cli_import_no_db(tmpdir, caplog):
    """
    If import is run with no db argument, db is created at DEFAULT_DB
    """
    caplog.set_level(logging.DEBUG)
    CliRunner.isolated_filesystem(tmpdir)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        with open(tmpdir / "test.jpg", "wb") as f:
            f.write(b"contents")

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--destination",
                "dest",
                "--priority",
                "10",
                str(tmpdir),
            ],
        )
        print("\nIMPORT no-db")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert (Path(fs) / cli.DEFAULT_DB).is_file()


def test_cli_import_no_changes(tmpdir, caplog):
    """
    If import is run with no new files, no new db is written
    """
    caplog.set_level(logging.DEBUG)
    CliRunner.isolated_filesystem(tmpdir)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        os.makedirs(tmpdir / "src")
        with open(tmpdir / "src" / "test.jpg", "wb") as f:
            f.write(b"contents")

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--destination",
                "dest",
                "--priority",
                "10",
                str(tmpdir / "src"),
            ],
        )
        print("\nIMPORT")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert (Path(fs) / cli.DEFAULT_DB).is_file()
        imported_dbs = list(Path(fs).glob(("*." + cli.DEFAULT_DB.split(".", 1)[1])))
        assert len(imported_dbs) == 1
        assert (
            "The database was not modified and will not be saved" not in caplog.messages
        )
        caplog.clear()

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--destination",
                "dest",
                "--priority",
                "10",
                str(tmpdir / "src"),
            ],
        )
        print("\nIMPORT no-new-files")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert (Path(fs) / cli.DEFAULT_DB).is_file()
        imported_dbs = list(Path(fs).glob(("*." + cli.DEFAULT_DB.split(".", 1)[1])))
        assert len(imported_dbs) == 1
        assert "The database was not modified and will not be saved" in caplog.messages
        caplog.clear()

        with open(tmpdir / "src" / "test2.jpg", "wb") as f:
            f.write(b"contents")

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--destination",
                "dest",
                "--priority",
                "10",
                str(tmpdir / "src"),
            ],
        )
        print("\nIMPORT new-file")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert (Path(fs) / cli.DEFAULT_DB).is_file()
        imported_dbs = list(Path(fs).glob(("*." + cli.DEFAULT_DB.split(".", 1)[1])))
        assert len(imported_dbs) == 2
        assert (
            "The database was not modified and will not be saved" not in caplog.messages
        )


def test_cli_collect_no_db(tmpdir, caplog):
    """
    collect must be given a database to collect to
    """
    caplog.set_level(logging.DEBUG)
    CliRunner.isolated_filesystem(tmpdir)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir):
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "collect",
                "--destination",
                str(tmpdir / "dest"),
            ],
        )
        print("\nCOLLECT no-db")
        print(result.output)
        print(result)
        assert result.exit_code == 2
        assert "does not exist" in result.output


def test_cli_verify_wrong_storage_type(tmpdir, caplog):
    """
    verify does not accept a nonexistent storage-type
    """
    caplog.set_level(logging.DEBUG)
    CliRunner.isolated_filesystem(tmpdir)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        caplog.set_level(logging.DEBUG)
        result = runner.invoke(
            cast(Group, cli.main),
            ["verify", "--storage-type", "nonexistent-asdf"],
        )
        print("\nVERIFY invalid storage type")
        print(result.output)
        print(result)
        assert result.exit_code == 2
        assert "Invalid value" in result.output
        check_dir_empty(fs)


def test_cli_verify_random_sample(tmpdir, caplog):
    """
    verify random-fraction checks a fraction of the database
    """
    caplog.set_level(logging.DEBUG)
    CliRunner.isolated_filesystem(tmpdir)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir):
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
                ],
                "uid2": [
                    {
                        "checksum": "asdf",
                        "source_path": str(tmpdir / "source2" / "b.jpg"),
                        "datetime": "2015:08:27 04:09:36.50",
                        "timestamp": 1440662976.5,
                        "file_size": 1024,
                        "store_path": "b.jpg",
                        "priority": 11,
                    },
                ],
                "uid3": [
                    {
                        "checksum": "ffff",
                        "source_path": str(tmpdir / "source1" / "c.jpg"),
                        "datetime": "2015:08:27 04:09:36.50",
                        "timestamp": 1440662976.5,
                        "file_size": 1024,
                        "store_path": "c.jpg",
                        "priority": 11,
                    },
                ],
                "uid4": [
                    {
                        "checksum": "beef",
                        "source_path": str(tmpdir / "source2" / "d.jpg"),
                        "datetime": "2015:08:27 04:09:36.50",
                        "timestamp": 1440662976.5,
                        "file_size": 1024,
                        "store_path": "d.jpg",
                        "priority": 11,
                    },
                ],
            },
            "command_history": {
                "2021-03-08_23-56-00Z": "photomanager create --db test.json"
            },
        }
        with open(tmpdir / "db.json", "w") as f:
            f.write(json.dumps(example_database))
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "verify",
                "--db",
                str(tmpdir / "db.json"),
                "--destination",
                str(tmpdir / "dest"),
                "--random-fraction",
                "0.5",
            ],
        )
        print("\nVERIFY 50%")
        print(result.output)
        print(result)
        assert result.exit_code == 1
        assert "Verifying 2 items" in caplog.messages
        assert "Checked 2 items" in caplog.messages
        assert "Found 0 incorrect and 2 missing items" in caplog.messages
