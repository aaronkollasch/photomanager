import asyncio
from asyncio import subprocess as subprocess_async
import logging
import pytest
from photomanager import pyexiftool_async

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
        pyexiftool_async.AsyncExifTool.make_chunks(
            chunks_test["it"], chunks_test["size"], chunks_test["init"]
        )
    )
    print(chunks)
    assert chunks == chunks_test["result"]


def nop_factory(json="{}"):
    async def nop_cse_f(*_, **kwargs):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)

        def protocol_factory():
            return subprocess_async.SubprocessStreamProtocol(limit=2 ** 16, loop=loop)

        transport, protocol = await loop.subprocess_exec(
            protocol_factory,
            "env",
            "-i",
            "bash",
            "--noprofile",
            "--norc",
            "-c",
            "read && echo -e '" + json + "{ready}'",
            **kwargs,
        )

        p = subprocess_async.Process(transport, protocol, loop)
        return p

    return nop_cse_f


def test_async_file_hasher_metadata(monkeypatch, caplog):
    nop_cse = nop_factory(
        '[{"SourceFile":"img1.jpg","EXIF:DateTimeOriginal":"2015:08:27 04:09:36"}]\n'
    )
    monkeypatch.setattr(subprocess_async, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    metadata = pyexiftool_async.AsyncExifTool(
        batch_size=10,
    ).get_metadata_batch(["img1.jpg"])
    print([(r.levelname, r) for r in caplog.records])
    print(metadata)
    assert not any(record.levelname == "WARNING" for record in caplog.records)
    assert len(metadata) == 1
    assert metadata["img1.jpg"]["EXIF:DateTimeOriginal"] == "2015:08:27 04:09:36"


def test_async_pyexiftool_error(monkeypatch, caplog):
    nop_cse = nop_factory("\n")
    monkeypatch.setattr(subprocess_async, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    metadata = pyexiftool_async.AsyncExifTool(
        batch_size=10,
    ).get_metadata_batch(["asdf.bin"])
    print([(r.levelname, r) for r in caplog.records])
    assert len(metadata) == 0
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert any("JSONDecodeError" in record.message for record in caplog.records)


def test_async_file_hasher_type_error(monkeypatch, caplog):
    nop_cse = nop_factory('{"SourceFile":"asdf.bin"}\n')
    monkeypatch.setattr(subprocess_async, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    metadata = pyexiftool_async.AsyncExifTool(
        batch_size=10,
    ).get_metadata_batch(["asdf.bin"])
    print([(r.levelname, r) for r in caplog.records])
    print(metadata)
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert any("TypeError" in record.message for record in caplog.records)
    assert len(metadata) == 0


def test_async_pyexiftool_interrupt(monkeypatch):
    async def communicate(_=None):
        await asyncio.sleep(5)
        return b"img.jpg checksum\n", b""

    monkeypatch.setattr(subprocess_async.Process, "communicate", communicate)
    tool = pyexiftool_async.AsyncExifTool(
        batch_size=10,
    )

    async def join(_=None):
        await asyncio.sleep(0.01)
        tool.terminate()

    monkeypatch.setattr(asyncio.Queue, "join", join)
    all_jobs = [[b"img.jpg"]]
    checksum_cache = asyncio.run(tool.execute_queue(all_params=all_jobs, num_files=1))
    assert len(checksum_cache) == 0
