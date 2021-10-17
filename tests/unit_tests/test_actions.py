import os
from pathlib import Path
import logging
import pytest
from click.testing import CliRunner
from photomanager.database import Database
from photomanager.actions import fileops, actions

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


def test_cli_verify_random_sample(tmpdir, caplog):
    """
    The random_fraction parameter in actions.verify will verify
    the specified fraction of the stored photos
    (rounded to the nearest integer)
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
    os.makedirs(tmpdir / "store")
    db = Database.from_dict(example_database)
    assert len(db.get_stored_photos()) == 4

    result = actions.verify(
        database=db,
        directory=tmpdir / "store",
        random_fraction=0.33,
    )
    print("\nVERIFY 33% (missing photos)")
    print(result)
    assert result["num_correct_photos"] == 0
    assert result["num_incorrect_photos"] == 0
    assert result["num_missing_photos"] == 1

    Path(tmpdir / "store" / "a.jpg").touch()
    Path(tmpdir / "store" / "b.jpg").touch()
    Path(tmpdir / "store" / "c.jpg").touch()
    Path(tmpdir / "store" / "d.jpg").touch()
    result = actions.verify(
        database=db,
        directory=tmpdir / "store",
        random_fraction=0.5,
    )
    print("\nVERIFY 50% (incorrect photos)")
    print(result)
    assert result["num_correct_photos"] == 0
    assert result["num_incorrect_photos"] == 2
    assert result["num_missing_photos"] == 0
