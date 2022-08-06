import logging
from pathlib import Path

import pytest

from photomanager.pyexiftool import AsyncExifTool, ExifTool
from photomanager.pyexiftool.pyexiftool import Singleton

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"
ALL_IMG_DIRS = pytest.mark.datafiles(
    FIXTURE_DIR / "A",
)


expected_tags = [
    {
        "filename": "img1.jpg",
        "tag": "EXIF:DateTimeOriginal",
        "value": "2015:08:01 18:28:36",
    },
    {
        "filename": "img1.png",
        "tag": "EXIF:SubSecTimeOriginal",
        "value": 90,
    },
    {
        "filename": "img4.jpg",
        "tag": "File:FileSize",
        "value": 759,
    },
]


@ALL_IMG_DIRS
@pytest.mark.parametrize("tag", expected_tags)
def test_pyexiftool_get_tag(datafiles, tag, caplog):
    caplog.set_level(logging.DEBUG)
    print(datafiles.listdir())
    with ExifTool() as exiftool:
        filename = str(datafiles / tag["filename"])
        new_tag = exiftool.get_tag(tag=tag["tag"], filename=filename)
        assert new_tag == tag["value"]
        new_tags = exiftool.get_tags(tags=[tag["tag"]], filename=filename)
        print(new_tags)
        assert new_tags == {"SourceFile": filename, tag["tag"]: tag["value"]}


def test_pyexiftool_nonexistent_file(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    with ExifTool() as exiftool:
        with pytest.warns(UserWarning):
            exiftool.start()
        tag = exiftool.get_tag(
            tag="EXIF:DateTimeOriginal", filename=str(tmpdir / "asdf.jpg")
        )
        assert tag is None
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any("empty string" in record.message for record in caplog.records)
        caplog.clear()
        tags = exiftool.get_tag_batch(
            tag="EXIF:DateTimeOriginal", filenames=[str(tmpdir / "asdf.jpg")]
        )
        assert tags == []
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any("empty string" in record.message for record in caplog.records)
        assert any("bad response" in record.message for record in caplog.records)
    assert not exiftool.running


def test_pyexiftool_terminate(caplog):
    caplog.set_level(logging.DEBUG)
    exiftool = ExifTool()
    exiftool.start()
    exiftool.terminate(wait_timeout=0)
    assert not exiftool.running
    exiftool.terminate()
    with pytest.raises(ValueError):
        exiftool.execute()
    assert not exiftool.running
    exiftool.start()
    assert exiftool.running
    del exiftool
    assert ExifTool.instance.running
    Singleton.clear(ExifTool)
    assert ExifTool.instance is None


@ALL_IMG_DIRS
def test_async_pyexiftool_get_tags(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    exiftool = AsyncExifTool(num_workers=2, batch_size=1)
    tags = exiftool.get_tags_batch(
        tags=list(set(d["tag"] for d in expected_tags)),
        filenames=list(set(str(datafiles / d["filename"]) for d in expected_tags)),
    )
    print(tags)
    for tag in expected_tags:
        filename = str(datafiles / tag["filename"])
        assert filename in tags
        assert tags[filename][tag["tag"]] == tag["value"]


def test_async_pyexiftool_nonexistent_file(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    exiftool = AsyncExifTool()
    tags = exiftool.get_tags_batch(
        tags=["EXIF:DateTimeOriginal"], filenames=[str(tmpdir / "asdf.jpg")]
    )
    assert tags == {}
    assert any(record.levelname == "WARNING" for record in caplog.records)
    exiftool.terminate()
    exiftool.terminate()
