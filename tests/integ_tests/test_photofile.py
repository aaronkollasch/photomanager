from pathlib import Path
import datetime
import pytest
from photomanager import database, pyexiftool

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"
ALL_IMG_DIRS = pytest.mark.datafiles(
    FIXTURE_DIR / "A",
    FIXTURE_DIR / "B",
    FIXTURE_DIR / "C",
    keep_top_dir=True,
)
photofile_expected_results = [
    database.PhotoFile(
        checksum="d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
        source_path="A/img1.jpg",
        datetime="2015:08:01 18:28:36.90",
        timestamp=1438468116.9,
        file_size=771,
    ),
    database.PhotoFile(
        checksum="3b39f47d51f63e54c76417ee6e04c34bd3ff5ac47696824426dca9e200f03666",
        source_path="A/img2.jpg",
        datetime="2015:08:01 18:28:36.99",
        timestamp=1438468116.99,
        file_size=771,
    ),
    database.PhotoFile(
        checksum="1e10df2e3abe4c810551525b6cb2eb805886de240e04cc7c13c58ae208cabfb9",
        source_path="A/img1.png",
        datetime="2015:08:01 18:28:36.90",
        timestamp=1438468116.9,
        file_size=382,
    ),
    database.PhotoFile(
        checksum="d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
        source_path="B/img1.jpg",
        datetime="2015:08:01 18:28:36.90",
        timestamp=1438468116.9,
        file_size=771,
    ),
    database.PhotoFile(
        checksum="e9fec87008fd240309b81c997e7ec5491fee8da7eb1a76fc39b8fcafa76bb583",
        source_path="B/img2.jpg",
        datetime="2015:08:01 18:28:36.99",
        timestamp=1438468116.99,
        file_size=789,
    ),
    database.PhotoFile(
        checksum="2aca4e78afbcebf2526ad8ac544d90b92991faae22499eec45831ef7be392391",
        source_path="C/img3.tiff",
        datetime="2018:08:01 19:28:36",
        timestamp=1533166116.0,
        file_size=506,
    ),
]


@ALL_IMG_DIRS
def test_photofile_from_file(datafiles):
    with pyexiftool.ExifTool():
        for pf in photofile_expected_results:
            pf = database.PhotoFile.from_dict(pf.to_dict())
            rel_path = pf.source_path
            pf.source_path = str(datafiles / rel_path)
            new_pf = database.PhotoFile.from_file(
                pf.source_path,
                tz_default=datetime.timezone(
                    datetime.timedelta(days=-1, seconds=72000)
                ),
            )
            assert new_pf == pf
