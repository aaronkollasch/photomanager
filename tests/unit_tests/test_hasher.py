import asyncio
from asyncio import subprocess as subprocess_async
from io import BytesIO
import pytest
from photomanager import hasher_async
from photomanager.hasher_async import file_checksum, AsyncFileHasher, HasherException

checksum_expected_results = [
    {
        "algorithm": "blake2b-256",
        "bytes": b"",
        "checksum": "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8",
    },
    {
        "algorithm": "blake2b-256",
        "bytes": b"\xff\xd8\xff\xe0",
        "checksum": "7d13007a8afed521cfc13306cbd6747bbc59556e3ca9514c8d94f900fbb56230",
    },
    {
        "algorithm": "sha256",
        "bytes": b"",
        "checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    },
    {
        "algorithm": "sha256",
        "bytes": b"\xff\xd8\xff\xe0",
        "checksum": "ba4f25bf16ba4be6bc7d3276fafeb67f9eb3c5df042bc3a405e1af15b921eed7",
    },
]


@pytest.mark.parametrize("checksum", checksum_expected_results)
def test_file_checksum(checksum):
    with BytesIO(checksum["bytes"]) as f:
        assert file_checksum(f, algorithm=checksum["algorithm"]) == checksum["checksum"]


def test_file_checksum_bad_algorithm():
    with pytest.raises(HasherException):
        file_checksum("asdf.txt", algorithm="md5")


def test_file_hasher_bad_algorithm():
    with pytest.raises(HasherException):
        AsyncFileHasher(algorithm="md5")


def test_file_hasher_command_available():
    assert AsyncFileHasher.cmd_available("b2sum")
    assert AsyncFileHasher.cmd_available(("b2sum", "-l", "256"))
    assert AsyncFileHasher.cmd_available(("sha256sum",))
    assert not AsyncFileHasher.cmd_available("nonexistent")
    assert not AsyncFileHasher.cmd_available(("sh", "-c", "exit 1"))


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
        AsyncFileHasher.make_chunks(
            chunks_test["it"], chunks_test["size"], chunks_test["init"]
        )
    )
    print(chunks)
    assert chunks == chunks_test["result"]


def test_async_file_hasher_interrupt(monkeypatch):
    async def communicate(_=None):
        await asyncio.sleep(5)
        return b"img.jpg checksum\n", b""

    monkeypatch.setattr(subprocess_async.Process, "communicate", communicate)
    hasher = hasher_async.AsyncFileHasher(
        algorithm="blake2b-256",
        use_async=True,
        batch_size=10,
    )

    async def join(_=None):
        await asyncio.sleep(0.01)
        hasher.terminate()

    monkeypatch.setattr(asyncio.Queue, "join", join)
    all_jobs = [hasher_async.FileHasherJob(file_paths=[b"img.jpg"])]
    checksum_cache = asyncio.run(hasher.execute_queue(all_jobs=all_jobs))
    assert len(checksum_cache) == 0
