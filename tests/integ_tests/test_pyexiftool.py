import logging
from pathlib import Path
from asyncio import subprocess as subprocess_async
import pytest
from photomanager import pyexiftool, pyexiftool_async

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
    with pyexiftool.ExifTool() as exiftool:
        assert (
            exiftool.get_tag(tag=tag["tag"], filename=str(datafiles / tag["filename"]))
            == tag["value"]
        )


def test_pyexiftool_nonexistent_file(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    with pyexiftool.ExifTool() as exiftool:
        with pytest.warns(UserWarning):
            exiftool.start()
        with pytest.raises(IndexError):
            exiftool.get_tag(
                tag="EXIF:DateTimeOriginal", filename=str(tmpdir / "asdf.jpg")
            )
        assert any(record.levelname == "WARNING" for record in caplog.records)
        caplog.clear()
        exiftool.get_tag_batch(
            tag="EXIF:DateTimeOriginal", filenames=[str(tmpdir / "asdf.jpg")]
        )
        assert any(record.levelname == "WARNING" for record in caplog.records)


def test_async_pyexiftool_error(tmpdir, monkeypatch, caplog):
    files = ["asdf.bin"]

    async def communicate(_=None):
        return b"\n", b""

    monkeypatch.setattr(subprocess_async.Process, "communicate", communicate)
    metadata = pyexiftool_async.AsyncExifTool(
        batch_size=10,
    ).get_metadata_batch(files)
    print([(r.levelname, r) for r in caplog.records])
    assert len(metadata) == 0

    async def communicate(_=None):
        return b"f/\x9c checksum\n", b""

    monkeypatch.setattr(subprocess_async.Process, "communicate", communicate)
    metadata = pyexiftool_async.AsyncExifTool(
        batch_size=10,
    ).get_metadata_batch(files)
    print([(r.levelname, r) for r in caplog.records])
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert len(metadata) == 0


@ALL_IMG_DIRS
def test_async_pyexiftool_get_tags(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    exiftool = pyexiftool_async.AsyncExifTool()
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
    exiftool = pyexiftool_async.AsyncExifTool()
    tags = exiftool.get_tags_batch(
        tags=["EXIF:DateTimeOriginal"], filenames=[str(tmpdir / "asdf.jpg")]
    )
    assert tags == {}
    assert any(record.levelname == "WARNING" for record in caplog.records)
