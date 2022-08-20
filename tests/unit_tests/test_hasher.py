import asyncio
import logging
from io import BytesIO

import pytest

from photomanager.hasher import (
    AsyncFileHasher,
    FileHasherJob,
    HashAlgorithm,
    HasherException,
    file_checksum,
)

from . import AsyncNopProcess

checksum_expected_results = [
    {
        "algorithm": HashAlgorithm.BLAKE2B_256,
        "bytes": b"",
        "checksum": "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8",
    },
    {
        "algorithm": HashAlgorithm.BLAKE2B_256,
        "bytes": b"\xff\xd8\xff\xe0",
        "checksum": "7d13007a8afed521cfc13306cbd6747bbc59556e3ca9514c8d94f900fbb56230",
    },
    {
        "algorithm": HashAlgorithm.SHA256,
        "bytes": b"",
        "checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    },
    {
        "algorithm": HashAlgorithm.SHA256,
        "bytes": b"\xff\xd8\xff\xe0",
        "checksum": "ba4f25bf16ba4be6bc7d3276fafeb67f9eb3c5df042bc3a405e1af15b921eed7",
    },
    {
        "algorithm": HashAlgorithm.BLAKE3,
        "bytes": b"",
        "checksum": "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
    },
    {
        "algorithm": HashAlgorithm.BLAKE3,
        "bytes": b"\xff\xd8\xff\xe0",
        "checksum": "4c1aae2ac7bedcc0449a6d5db09be996889d9163f48142a9f3a3a49602447dfe",
    },
]


@pytest.mark.parametrize("checksum", checksum_expected_results)
def test_file_checksum_fd(checksum):
    with BytesIO(checksum["bytes"]) as f:
        assert file_checksum(f, algorithm=checksum["algorithm"]) == checksum["checksum"]
        assert not f.closed


@pytest.mark.parametrize("checksum", checksum_expected_results)
def test_file_checksum_path(checksum, tmpdir):
    with open(tmpdir / "test.bin", "wb") as f:
        f.write(checksum["bytes"])
    assert (
        file_checksum(tmpdir / "test.bin", algorithm=checksum["algorithm"])
        == checksum["checksum"]
    )


@pytest.mark.parametrize("checksum", checksum_expected_results)
def test_async_checksum_path(checksum, tmpdir):
    files = [tmpdir / "test.bin"]
    with open(files[0], "wb") as f:
        f.write(checksum["bytes"])
    checksum_cache = AsyncFileHasher(
        algorithm=checksum["algorithm"], use_async=True
    ).check_files(files, pbar_unit="B")
    print(checksum_cache)
    assert len(checksum_cache) == len(files)
    assert files[0] in checksum_cache
    assert checksum_cache[files[0]] == checksum["checksum"]


# noinspection PyTypeChecker
def test_file_checksum_bad_algorithm():
    with pytest.raises(HasherException):
        file_checksum("asdf.txt", algorithm="md5")


# noinspection PyTypeChecker
def test_async_file_hasher_bad_algorithm():
    with pytest.raises(HasherException):
        AsyncFileHasher(algorithm="md5")


def test_async_file_hasher_img(monkeypatch, caplog):
    async def nop_cse(*_, **__):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)
        return AsyncNopProcess(b"ba4f25bf16ba4be6bc7d3276fafeba img1.jpg\n", b"")

    monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256,
        use_async=True,
        batch_size=1,
    ).check_files(["img1.jpg"], pbar_unit="it")
    print([(r.levelname, r) for r in caplog.records])
    print(checksum_cache)
    assert not any(record.levelname == "WARNING" for record in caplog.records)
    assert len(checksum_cache) == 1
    assert "img1.jpg" in checksum_cache
    assert checksum_cache["img1.jpg"] == "ba4f25bf16ba4be6bc7d3276fafeba"


def test_async_file_hasher_empty(monkeypatch, caplog):
    async def nop_cse(*_, **__):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)
        return AsyncNopProcess(b"\n", b"")

    monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256,
        use_async=True,
        batch_size=10,
    ).check_files(["asdf.bin"], pbar_unit="it")
    print([(r.levelname, r) for r in caplog.records])
    print(checksum_cache)
    assert not any(record.levelname == "WARNING" for record in caplog.records)
    assert len(checksum_cache) == 0


def test_async_file_hasher_unicode_error(monkeypatch, caplog):
    async def nop_cse(*_, **__):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)
        return AsyncNopProcess(b"f/\x9c file.txt\n", b"")

    monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
    caplog.set_level(logging.DEBUG)
    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256,
        use_async=True,
        batch_size=10,
    ).check_files(["asdf.bin"], pbar_unit="it")
    print([(r.levelname, r) for r in caplog.records])
    print(checksum_cache)
    assert any(record.levelname == "WARNING" for record in caplog.records)
    assert any("UnicodeDecodeError" in record.message for record in caplog.records)
    assert len(checksum_cache) == 0


def test_async_file_hasher_interrupt(monkeypatch):
    async def nop_cse(*_, **__):
        loop = asyncio.events.get_event_loop()
        loop.set_debug(True)
        return AsyncNopProcess(b"checksum img.jpg\n", b"", final_delay=5)

    monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
    hasher = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256,
        use_async=True,
        batch_size=10,
    )

    async def join(_=None):
        await asyncio.sleep(0.01)
        hasher.terminate()

    monkeypatch.setattr(asyncio.Queue, "join", join)
    all_jobs = [FileHasherJob(file_paths=[b"img.jpg"])]
    checksum_cache = asyncio.run(hasher.execute_queue(all_jobs=all_jobs))
    print(checksum_cache)
    assert len(checksum_cache) == 0
