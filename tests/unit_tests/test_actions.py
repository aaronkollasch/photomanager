import logging
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from photomanager.actions import actions, fileops
from photomanager.database import Database
from photomanager.photofile import PhotoFile

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"


def check_dir_empty(dir_path):
    cwd_files = list(Path(dir_path).glob("*"))
    print(cwd_files)
    assert len(cwd_files) == 0


class TestFileOps:
    @pytest.mark.datafiles(FIXTURE_DIR)
    def test_list_files_stdin_source(self, datafiles, caplog):
        """
        list_files accepts multiple directories piped to stdin when source == "-"
        """
        caplog.set_level(logging.DEBUG)
        runner = CliRunner()
        with runner.isolation(
            input=str(datafiles / "A") + "\n" + str(datafiles / "B") + "\n"
        ):
            files = fileops.list_files(source="-")
        print(files)
        assert set(files.keys()) == {
            str(datafiles / "A" / "img1.jpg"),
            str(datafiles / "A" / "img1.png"),
            str(datafiles / "A" / "img2.jpg"),
            str(datafiles / "A" / "img4.jpg"),
            str(datafiles / "B" / "img1.jpg"),
            str(datafiles / "B" / "img2.jpg"),
            str(datafiles / "B" / "img4.jpg"),
        }

    @pytest.mark.datafiles(
        FIXTURE_DIR / "A",
        keep_top_dir=True,
    )
    def test_list_files_source(self, datafiles, caplog):
        """
        list_files lists a directory in the source argument
        """
        caplog.set_level(logging.DEBUG)
        files = fileops.list_files(source=str(datafiles / "A"))
        print(files)
        assert set(files.keys()) == {
            str(datafiles / "A" / "img1.jpg"),
            str(datafiles / "A" / "img1.png"),
            str(datafiles / "A" / "img2.jpg"),
            str(datafiles / "A" / "img4.jpg"),
        }

    @pytest.mark.datafiles(FIXTURE_DIR)
    def test_list_files_paths_exclude(self, datafiles, caplog):
        """
        The list_files exclude argument removes filenames matching the patterns
        """
        caplog.set_level(logging.DEBUG)
        files = fileops.list_files(
            paths=[str(datafiles)], exclude=["img1", ".tiff", "D"]
        )
        print(files)
        assert set(files.keys()) == {
            str(datafiles / "A" / "img2.jpg"),
            str(datafiles / "A" / "img4.jpg"),
            str(datafiles / "B" / "img2.jpg"),
            str(datafiles / "B" / "img4.jpg"),
        }

    @pytest.mark.datafiles(FIXTURE_DIR / "A" / "img1.png")
    def test_list_files_file(self, datafiles, caplog):
        """
        list_files will return a file provided with the file argument
        """
        caplog.set_level(logging.DEBUG)
        files = fileops.list_files(file=str(datafiles / "img1.png"))
        print(files)
        assert set(files.keys()) == {
            str(datafiles / "img1.png"),
        }

    @pytest.mark.datafiles(
        FIXTURE_DIR / "A" / "img1.png",
        FIXTURE_DIR / "A" / "img1.jpg",
    )
    def test_list_files_stdin_file(self, datafiles, caplog):
        """
        list_files accepts multiple files piped to stdin when file == "-"
        """
        caplog.set_level(logging.DEBUG)
        runner = CliRunner()
        with runner.isolation(
            input=f"{datafiles / 'img1.png'}\n{datafiles / 'img1.jpg'}\n"
        ):
            files = fileops.list_files(file="-")
        print(files)
        assert set(files.keys()) == {
            str(datafiles / "img1.jpg"),
            str(datafiles / "img1.png"),
        }

    @pytest.mark.datafiles(
        FIXTURE_DIR / "A",
        keep_top_dir=True,
    )
    def test_list_files_exclude_files(self, datafiles, caplog):
        """
        list_files excludes files with exact paths provided to exclude_files
        """
        caplog.set_level(logging.DEBUG)
        files = fileops.list_files(
            source=str(datafiles / "A"),
            exclude_files=[
                str(datafiles) + "/A/img1.jpg",
                datafiles / "A" / "img4.jpg",
                "A/img1.png",
                "img2.jpg",
            ],
        )
        print(files)
        assert set(files.keys()) == {
            str(datafiles / "A" / "img1.png"),
            str(datafiles / "A" / "img2.jpg"),
        }

    def test_list_files_non_file(self, tmpdir, caplog):
        """
        list_files excludes paths that are not files
        """
        caplog.set_level(logging.DEBUG)
        files = fileops.list_files(paths=[tmpdir])
        print(files)
        assert len(files) == 0
        os.makedirs(tmpdir / "not_a_file.jpg/test1.jpg")
        with open(tmpdir / "not_a_file.jpg" / "test2.jpg", "w") as f:
            f.write("test")
        files = fileops.list_files(paths=[tmpdir])
        print(files)
        assert len(files) == 1
        assert next(iter(files)) == str(tmpdir / "not_a_file.jpg" / "test2.jpg")
        assert any("not a file" in m for m in caplog.messages)

    def test_index_photos_empty_list(self, caplog):
        """
        async index_photos does not error if no files are given
        """
        caplog.set_level(logging.DEBUG)
        photos = fileops.index_photos(files=[], storage_type="SSD")
        print(photos)
        assert len(photos) == 0

    @pytest.mark.datafiles(
        FIXTURE_DIR / "B",
        keep_top_dir=True,
    )
    def test_copy_photos(self, datafiles, caplog):
        """
        copy_photos will copy the supplied PhotoFiles to the destination folder
        using the provided relative store path, or PhotoFile.sto if no path is provided.
        """
        caplog.set_level(logging.DEBUG)
        photos_to_copy = [
            (
                PhotoFile(
                    chk="deadbeef",
                    src="B/img2.jpg",
                    sto="2015/08/2015-08-01_img2.jpg",
                    dt="2015:08:01 18:28:36.99",
                    ts=1438468116.99,
                    fsz=789,
                    tzo=-14400.0,
                ),
                None,
            ),
            (
                PhotoFile(
                    chk="deadbeef",
                    src="B/img4.jpg",
                    dt="2018:08:01 20:28:36",
                    ts=1533169716.0,
                    fsz=777,
                    tzo=-14400.0,
                ),
                "2018/08/2018-08-01_img4.jpg",
            ),
            (
                PhotoFile(
                    chk="deadbeef",
                    src="B/img_missing.jpg",
                    sto="2018/08/2018-08-08_img_missing.jpg",
                    dt="2018:08:01 20:28:36",
                    ts=1533169716.0,
                    fsz=777,
                    tzo=-14400.0,
                ),
                None,
            ),
        ]
        for pf, rel_store_path in photos_to_copy:
            pf.src = str(datafiles / pf.src)
        num_copied_photos, total_copy_size, num_error_photos = fileops.copy_photos(
            datafiles / "dest", photos_to_copy
        )
        print(num_copied_photos, total_copy_size, num_error_photos)
        assert num_copied_photos == 2
        assert total_copy_size == 789 + 777
        assert num_error_photos == 1
        assert os.listdir(datafiles / "dest/2015/08") == ["2015-08-01_img2.jpg"]
        assert os.listdir(datafiles / "dest/2018/08") == ["2018-08-01_img4.jpg"]

    @pytest.mark.datafiles(
        FIXTURE_DIR / "B",
        keep_top_dir=True,
    )
    def test_remove_photos(self, datafiles, caplog):
        """
        remove_photos will remove the supplied PhotoFiles if they are not missing
        """
        caplog.set_level(logging.DEBUG)
        photos_to_remove = [
            PhotoFile(
                chk="deadbeef",
                src="B/img2.jpg",
                dt="2015:08:01 18:28:36.99",
                ts=1438468116.99,
                fsz=789,
                tzo=-14400.0,
            ),
            PhotoFile(
                chk="deadbeef",
                src="B/img4.jpg",
                dt="2018:08:01 20:28:36",
                ts=1533169716.0,
                fsz=777,
                tzo=-14400.0,
            ),
            PhotoFile(
                chk="deadbeef",
                src="B/img_missing.jpg",
                dt="2018:08:01 20:28:36",
                ts=1533169716.0,
                fsz=777,
                tzo=-14400.0,
            ),
        ]
        for pf in photos_to_remove:
            pf.sto = str(datafiles / pf.src)
        num_removed_photos, num_missing_photos = fileops.remove_photos(
            directory=datafiles, photos=photos_to_remove
        )
        assert num_removed_photos == 2
        assert num_missing_photos == 1
        assert os.listdir(datafiles / "B") == ["img1.jpg"]


def test_verify_random_sample(tmpdir, caplog):
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
