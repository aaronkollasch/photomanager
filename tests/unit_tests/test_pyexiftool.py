import asyncio
from asyncio import subprocess as subprocess_async
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
