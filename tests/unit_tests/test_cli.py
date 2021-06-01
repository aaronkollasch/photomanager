import sys
from pathlib import Path
import logging
import pytest
from click.testing import CliRunner
from photomanager import cli

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

    monkeypatch.setattr(cli, "__name__", "__main__")
    monkeypatch.setattr(sys, "argv", ["photomanager", "--version"])
    with pytest.raises(SystemExit) as exit_type:
        cli._init()
    assert exit_type.value.code == 0
