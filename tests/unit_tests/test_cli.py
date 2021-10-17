import sys
from pathlib import Path
import logging
import json
import pytest
from typing import cast
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


def test_cli_index_collect_no_db(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    CliRunner.isolated_filesystem(tmpdir)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--priority",
                "10",
                str(tmpdir),
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
    caplog.set_level(logging.DEBUG)
    CliRunner.isolated_filesystem(tmpdir)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
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


def test_cli_collect_no_db(tmpdir, caplog):
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
        assert result.exit_code == 1
        assert isinstance(result.exception, FileNotFoundError)
