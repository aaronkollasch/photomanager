from pathlib import Path
import logging
import pytest
from click.testing import CliRunner
from photomanager.actions import fileops

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"


def check_dir_empty(dir_path):
    cwd_files = list(Path(dir_path).glob("*"))
    print(cwd_files)
    assert len(cwd_files) == 0


class TestFileOps:
    @pytest.mark.datafiles(
        FIXTURE_DIR / "A",
        keep_top_dir=True,
    )
    def test_list_files_stdin_source(self, datafiles, caplog):
        caplog.set_level(logging.DEBUG)
        runner = CliRunner()
        with runner.isolation(input=str(datafiles / "A") + "\n"):
            files = fileops.list_files(source="-")
            assert len(files) == 4

    @pytest.mark.datafiles(
        FIXTURE_DIR / "C",
        keep_top_dir=True,
    )
    def test_list_files_source(self, datafiles, caplog):
        caplog.set_level(logging.DEBUG)
        files = fileops.list_files(source=str(datafiles / "C"))
        assert len(files) == 1

    @pytest.mark.datafiles(
        FIXTURE_DIR / "A",
        keep_top_dir=True,
    )
    def test_list_files_paths_exclude(self, datafiles, caplog):
        caplog.set_level(logging.DEBUG)
        files = fileops.list_files(paths=[str(datafiles / "A")], exclude=["img1"])
        assert len(files) == 2

    @pytest.mark.datafiles(FIXTURE_DIR / "A" / "img1.png")
    def test_list_files_file(self, datafiles, caplog):
        caplog.set_level(logging.DEBUG)
        files = fileops.list_files(file=str(datafiles / "img1.png"))
        assert len(files) == 1

    @pytest.mark.datafiles(
        FIXTURE_DIR / "A" / "img1.png",
        FIXTURE_DIR / "A" / "img1.jpg",
    )
    def test_list_files_stdin_file(self, datafiles, caplog):
        caplog.set_level(logging.DEBUG)
        runner = CliRunner()
        with runner.isolation(
            input=f"{datafiles / 'img1.png'}\n{datafiles / 'img1.jpg'}\n"
        ):
            files = fileops.list_files(file="-")
        assert len(files) == 2
