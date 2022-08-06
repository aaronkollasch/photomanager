from datetime import timedelta, timezone
from pathlib import Path

import pytest

from photomanager.photofile import PhotoFile
from photomanager.pyexiftool import ExifTool

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"
ALL_IMG_DIRS = pytest.mark.datafiles(
    FIXTURE_DIR / "A",
    FIXTURE_DIR / "B",
    FIXTURE_DIR / "C",
    keep_top_dir=True,
)
photofile_expected_results = [
    PhotoFile(
        chk="d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
        src="A/img1.jpg",
        dt="2015:08:01 18:28:36.90",
        ts=1438468116.9,
        fsz=771,
        tzo=-14400.0,
    ),
    PhotoFile(
        chk="3b39f47d51f63e54c76417ee6e04c34bd3ff5ac47696824426dca9e200f03666",
        src="A/img2.jpg",
        dt="2015:08:01 18:28:36.99",
        ts=1438450116.99,
        fsz=771,
        tzo=3600.0,
    ),
    PhotoFile(
        chk="1e10df2e3abe4c810551525b6cb2eb805886de240e04cc7c13c58ae208cabfb9",
        src="A/img1.png",
        dt="2015:08:01 18:28:36.90",
        ts=1438453716.9,
        fsz=382,
        tzo=0.0,
    ),
    PhotoFile(
        chk="79ac4a89fb3d81ab1245b21b11ff7512495debca60f6abf9afbb1e1fbfe9d98c",
        src="A/img4.jpg",
        dt="2018:08:01 20:28:36",
        ts=1533169716.0,
        fsz=759,
        tzo=-14400.0,
    ),
    PhotoFile(
        chk="d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
        src="B/img1.jpg",
        dt="2015:08:01 18:28:36.90",
        ts=1438468116.9,
        fsz=771,
        tzo=-14400.0,
    ),
    PhotoFile(
        chk="e9fec87008fd240309b81c997e7ec5491fee8da7eb1a76fc39b8fcafa76bb583",
        src="B/img2.jpg",
        dt="2015:08:01 18:28:36.99",
        ts=1438468116.99,
        fsz=789,
        tzo=-14400.0,
    ),
    PhotoFile(
        chk="2b0f304f86655ebd04272cc5e7e886e400b79a53ecfdc789f75dd380cbcc8317",
        src="B/img4.jpg",
        dt="2018:08:01 20:28:36",
        ts=1533169716.0,
        fsz=777,
        tzo=-14400.0,
    ),
    PhotoFile(
        chk="2aca4e78afbcebf2526ad8ac544d90b92991faae22499eec45831ef7be392391",
        src="C/img3.tiff",
        dt="2018:08:01 19:28:36",
        ts=1533166116.0,
        fsz=506,
        tzo=-14400.0,
    ),
]


@ALL_IMG_DIRS
def test_photofile_from_file(datafiles):
    with ExifTool():
        for pf in photofile_expected_results:
            pf = PhotoFile.from_dict(pf.to_dict())
            rel_path = pf.src
            pf.src = str(datafiles / rel_path)
            new_pf = PhotoFile.from_file(
                pf.src,
                tz_default=timezone(timedelta(seconds=pf.tzo)),
            )
            assert new_pf == pf
