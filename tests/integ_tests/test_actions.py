import logging
from datetime import timedelta, timezone
from pathlib import Path

import pytest

from photomanager.actions import fileops
from photomanager.hasher import HashAlgorithm
from photomanager.photofile import PhotoFile

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"


class TestFileOps:
    @pytest.mark.datafiles(
        FIXTURE_DIR / "A",
        keep_top_dir=True,
    )
    def test_index_photos(self, datafiles, caplog):
        """
        index_photos will make PhotoFiles for the supplied list of paths
        and skip nonexistent photos
        """
        caplog.set_level(logging.DEBUG)
        files = [
            str(datafiles / "A" / "img1.jpg"),
            Path(datafiles / "A" / "img1.png"),
            str(datafiles / "A" / "img_nonexistent.jpg"),
            str(datafiles / "A" / "img4.jpg"),
            str(datafiles / "A" / "img2.jpg"),
        ]
        expected_photos = [
            PhotoFile(
                chk="d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
                src=str(datafiles / "A" / "img1.jpg"),
                dt="2015:08:01 18:28:36.90",
                ts=1438468116.9,
                fsz=771,
                sto="",
                prio=11,
                tzo=-14400.0,
            ),
            PhotoFile(
                chk="1e10df2e3abe4c810551525b6cb2eb805886de240e04cc7c13c58ae208cabfb9",
                src=str(datafiles / "A" / "img1.png"),
                dt="2015:08:01 18:28:36.90",
                ts=1438468116.9,
                fsz=382,
                sto="",
                prio=11,
                tzo=-14400.0,
            ),
            None,
            PhotoFile(
                chk="79ac4a89fb3d81ab1245b21b11ff7512495debca60f6abf9afbb1e1fbfe9d98c",
                src=str(datafiles / "A" / "img4.jpg"),
                dt="2018:08:01 20:28:36",
                ts=1533169716.0,
                fsz=759,
                sto="",
                prio=11,
                tzo=-14400.0,
            ),
            PhotoFile(
                chk="3b39f47d51f63e54c76417ee6e04c34bd3ff5ac47696824426dca9e200f03666",
                src=str(datafiles / "A" / "img2.jpg"),
                dt="2015:08:01 18:28:36.99",
                ts=1438468116.99,
                fsz=771,
                sto="",
                prio=11,
                tzo=-14400.0,
            ),
        ]
        photos = fileops.index_photos(
            files=files, priority=11, tz_default=timezone(timedelta(seconds=-14400.0))
        )
        print(photos)
        print(len(photos))
        assert photos == expected_photos

    @pytest.mark.datafiles(
        FIXTURE_DIR / "A",
        keep_top_dir=True,
    )
    def test_hash_stored_photos(self, datafiles, caplog):
        """
        hash_stored_photos will return a dict of filenames:hashes
        given a list of stored photos.
        """
        caplog.set_level(logging.DEBUG)
        stored_photos = [
            PhotoFile(
                chk="d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
                src=str(datafiles / "A" / "img1.jpg"),
                dt="2015:08:01 18:28:36.90",
                ts=1438468116.9,
                fsz=771,
                sto="A/img1.jpg",
                prio=11,
                tzo=None,
            ),
            PhotoFile(
                chk="1e10df2e3abe4c810551525b6cb2eb805886de240e04cc7c13c58ae208cabfb9",
                src=str(datafiles / "A" / "img1.png"),
                dt="2015:08:01 18:28:36.90",
                ts=1438468116.9,
                fsz=382,
                sto="A/img1.png",
                prio=11,
                tzo=None,
            ),
            PhotoFile(
                chk="3b39f47d51f63e54c76417ee6e04c34bd3ff5ac47696824426dca9e200f03666",
                src=str(datafiles / "A" / "img2.jpg"),
                dt="2015:08:01 18:28:36.99",
                ts=1438468116.99,
                fsz=771,
                sto="A/img2.jpg",
                prio=11,
                tzo=None,
            ),
            PhotoFile(
                chk="79ac4a89fb3d81ab1245b21b11ff7512495debca60f6abf9afbb1e1fbfe9d98c",
                src=str(datafiles / "A" / "img4.jpg"),
                dt="2018:08:01 20:28:36",
                ts=1533169716.0,
                fsz=759,
                sto="A/img4.jpg",
                prio=11,
                tzo=None,
            ),
            PhotoFile(
                chk="79ac4a89fb3d81ab1245b21b11ff7512495debca60f6abf9afbb1e1fbfe9d98c",
                src=str(datafiles / "A" / "img_nonexistent.jpg"),
                dt="2018:08:01 20:28:36",
                ts=1533169716.0,
                fsz=759,
                sto="A/img_nonexistent.jpg",
                prio=11,
                tzo=None,
            ),
        ]
        expected_hashes = {
            pf.src: pf.chk for pf in stored_photos if "nonexistent" not in pf.src
        }
        photos = fileops.hash_stored_photos(
            photos=stored_photos,
            directory=datafiles,
            hash_algorithm=HashAlgorithm.BLAKE2B_256,
        )
        print(photos)
        assert photos == expected_hashes
