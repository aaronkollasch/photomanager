import asyncio
from asyncio import subprocess as subprocess_async
import logging
import pytest
from photomanager.pyexiftool import ExifTool, AsyncExifTool
from . import NopProcess, AsyncNopProcess

chunker_expected_results = [
    {
        "it": ("a", "b", "c", "d", "e", "f"),
        "size": 2,
        "init": ("asdf", "-t"),
        "result": [
            ["asdf", "-t", "a", "b"],
            ["asdf", "-t", "c", "d"],
            ["asdf", "-t", "e", "f"],
        ],
    },
    {
        "it": ("a", "b", "c", "d", "e"),
        "size": 2,
        "init": ("asdf", "-t"),
        "result": [
            ["asdf", "-t", "a", "b"],
            ["asdf", "-t", "c", "d"],
            ["asdf", "-t", "e"],
        ],
    },
    {
        "it": ("a", "b", "c", "d", "e"),
        "size": 3,
        "init": ("asdf", "-t"),
        "result": [
            ["asdf", "-t", "a", "b", "c"],
            ["asdf", "-t", "d", "e"],
        ],
    },
    {
        "it": ("a", "b", "c", "d", "e"),
        "size": 3,
        "init": (),
        "result": [
            ["a", "b", "c"],
            ["d", "e"],
        ],
    },
    {
        "it": ("a", "b"),
        "size": 3,
        "init": ("asdf", "-t"),
        "result": [
            ["asdf", "-t", "a", "b"],
        ],
    },
    {
        "it": (),
        "size": 3,
        "init": ("asdf", "-t"),
        "result": [],
    },
]


@pytest.mark.parametrize("chunks_test", chunker_expected_results)
def test_make_chunks(chunks_test):
    chunks = list(
        AsyncExifTool.make_chunks(
            chunks_test["it"], chunks_test["size"], chunks_test["init"]
        )
    )
    print(chunks)
    assert chunks == chunks_test["result"]


def test_async_pyexiftool_metadata(monkeypatch, caplog):
    async def nop_cse(*_, **__):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)
        return AsyncNopProcess(
            stdout_messages=(
                b'[{"SourceFile":"img1.jpg","EXIF:DateTimeOriginal":"2015:08:27 04:09:36"}]\n',
                b"{ready}",
            )
        )

    monkeypatch.setattr(subprocess_async, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    metadata = AsyncExifTool(batch_size=10).get_metadata_batch(["img1.jpg"])
    print([(r.levelname, r) for r in caplog.records])
    print(metadata)
    assert not any(record.levelname == "WARNING" for record in caplog.records)
    assert len(metadata) == 1
    assert metadata["img1.jpg"]["EXIF:DateTimeOriginal"] == "2015:08:27 04:09:36"


def test_async_pyexiftool_error(monkeypatch, caplog):
    async def nop_cse(*_, **__):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)
        return AsyncNopProcess(
            stdout_messages=(
                b"\n",
                b"{ready}",
            )
        )

    monkeypatch.setattr(subprocess_async, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    metadata = AsyncExifTool(batch_size=10).get_metadata_batch(["asdf.bin"])
    print([(r.levelname, r) for r in caplog.records])
    assert len(metadata) == 0
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert any("JSONDecodeError" in record.message for record in caplog.records)


def test_async_pyexiftool_type_error(monkeypatch, caplog):
    async def nop_cse(*_, **__):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)
        return AsyncNopProcess(
            stdout_messages=(
                b'{"SourceFile":"asdf.bin"}\n',
                b"{ready}",
            )
        )

    monkeypatch.setattr(subprocess_async, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    metadata = AsyncExifTool(batch_size=10).get_metadata_batch(["asdf.bin"])
    print([(r.levelname, r) for r in caplog.records])
    print(metadata)
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert any("TypeError" in record.message for record in caplog.records)
    assert len(metadata) == 0


def test_async_pyexiftool_interrupt(monkeypatch):
    async def nop_cse(*_, **__):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)
        return AsyncNopProcess(
            stdout_messages=(
                b'{"SourceFile":"asdf.bin"}\n',
                b"{ready}",
            ),
            message_delay=5,
        )

    monkeypatch.setattr(subprocess_async, "create_subprocess_exec", nop_cse)
    tool = AsyncExifTool(batch_size=10)

    async def join(_=None):
        await asyncio.sleep(0.01)
        tool.terminate()

    monkeypatch.setattr(asyncio.Queue, "join", join)
    all_jobs = [[b"img.jpg"]]
    metadata = asyncio.run(tool.execute_queue(all_params=all_jobs, num_files=1))
    assert len(metadata) == 0


expected_metadata = [
    {
        "filename": "img1.jpg",
        "exiftool_output": "[{}]",
        "value": {},
    },
    {
        "filename": "img1.jpg",
        "exiftool_output": '[{"SourceFile":"img1.jpg"}]',
        "value": {"SourceFile": "img1.jpg"},
    },
]


@pytest.mark.parametrize("metadata", expected_metadata)
def test_pyexiftool_get_metadata(metadata, caplog):
    caplog.set_level(logging.DEBUG)
    exiftool = ExifTool(executable_="true")
    exiftool._process = NopProcess(
        stdout_messages=(
            metadata["exiftool_output"].encode(),
            b"{ready}",
        )
    )
    exiftool.running = True
    assert exiftool.get_metadata(filename=metadata["filename"]) == metadata["value"]


def test_pyexiftool_get_metadata_batch(caplog):
    caplog.set_level(logging.DEBUG)
    exiftool = ExifTool(executable_="true")
    exiftool._process = NopProcess(
        stdout_messages=(
            b'[{"SourceFile":"img1.jpg"},{"SourceFile":"img2.jpg"}]',
            b"{ready}",
        )
    )
    exiftool.running = True
    assert exiftool.get_metadata_batch(filenames=["img1.jpg", "img2.jpg"]) == [
        {"SourceFile": "img1.jpg"},
        {"SourceFile": "img2.jpg"},
    ]
    with pytest.raises(TypeError):
        exiftool.get_tags_batch(None, "img1.jpg")
    with pytest.raises(TypeError):
        exiftool.get_tags_batch("EXIF:DateTimeOriginal", None)
