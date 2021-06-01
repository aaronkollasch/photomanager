import sys
from pathlib import Path
import logging
import subprocess
import json
import pytest
from click.testing import CliRunner
from photomanager import cli, database, version

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"


@pytest.mark.datafiles(
    FIXTURE_DIR / "A",
    keep_top_dir=True,
)
def test_cli_list_files_stdin_source(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolation(input=str(datafiles / "A") + "\n"):
        files = cli.list_files(source="-")
        assert len(files) == 4


@pytest.mark.datafiles(
    FIXTURE_DIR / "C",
    keep_top_dir=True,
)
def test_cli_list_files_source(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    files = cli.list_files(source=str(datafiles / "C"))
    assert len(files) == 1


@pytest.mark.datafiles(
    FIXTURE_DIR / "A",
    keep_top_dir=True,
)
def test_cli_list_files_paths_exclude(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    files = cli.list_files(paths=[str(datafiles / "A")], exclude=["img1"])
    assert len(files) == 2


@pytest.mark.datafiles(FIXTURE_DIR / "A" / "img1.png")
def test_cli_list_files_file(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    files = cli.list_files(file=str(datafiles / "img1.png"))
    assert len(files) == 1


@pytest.mark.datafiles(
    FIXTURE_DIR / "A" / "img1.png",
    FIXTURE_DIR / "A" / "img1.jpg",
)
def test_cli_list_files_stdin_file(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolation(
        input=str(datafiles / "img1.png") + "\n" + str(datafiles / "img1.jpg") + "\n"
    ):
        files = cli.list_files(file="-")
    assert len(files) == 2


def test_cli_main(monkeypatch):
    from photomanager import __main__

    monkeypatch.setattr(__main__, "__name__", "__main__")
    monkeypatch.setattr(sys, "argv", ["photomanager", "--version"])
    with pytest.raises(SystemExit) as exit_type:
        __main__._init()
    assert exit_type.value.code == 0

    monkeypatch.setattr(sys, "argv", ["photomanager", "--version"])
    with pytest.raises(SystemExit) as exit_type:
        __main__.main()
    assert exit_type.value.code == 0

    monkeypatch.setattr(cli, "__name__", "__main__")
    monkeypatch.setattr(sys, "argv", ["photomanager", "--version"])
    with pytest.raises(SystemExit) as exit_type:
        cli._init()
    assert exit_type.value.code == 0


def test_cli_exit_codes(monkeypatch):
    monkeypatch.setattr(cli, "__name__", "__main__")
    monkeypatch.setattr(sys, "argv", ["photomanager", "index"])
    with pytest.raises(SystemExit) as exit_type:
        cli._init()
    assert exit_type.value.code == 1


def test_cli_exit_code_no_files(tmpdir):
    assert cli._index(["--db", str(tmpdir / "none.json")], standalone_mode=False) == 1


def test_photomanager_bin_install():
    p = subprocess.Popen(
        ["photomanager", "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate()
    print(stdout, stderr)
    print("exit", p.returncode)
    assert p.returncode == 0
    assert stdout.strip() == f"photomanager {version}".encode()


def test_photomanager_bin_error(tmpdir):
    p = subprocess.Popen(
        ["photomanager", "stats", "--db", str(tmpdir / "none.json")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate()
    print(stdout, stderr)
    print("exit", p.returncode)
    assert p.returncode == 1
    assert b"FileNotFoundError" in stderr


def test_cli_create(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as td:
        result = runner.invoke(cli.main, ["create", "--db", "test.json"])
        print(result.output)
        print(list(Path(td).glob("**/*")))
        assert result.exit_code == 0
        with open(Path(td) / "test.json") as f:
            s = f.read()
        print(s)
        d = json.loads(s)
        assert d["photo_db"] == {}
        assert d["version"] == database.Database.VERSION
        assert d["hash_algorithm"] == database.DEFAULT_HASH_ALGO


def test_cli_index_directory_db(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    result = runner.invoke(
        cli.main,
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


def test_cli_index_nothing(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    result = runner.invoke(
        cli.main,
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