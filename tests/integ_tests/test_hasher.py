import logging
import random
from io import BytesIO
from photomanager.hasher import AsyncFileHasher, file_checksum, HashAlgorithm

checksums = [
    (b"", "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"),
    (
        b"\xff\xd8\xff\xe0",
        "7d13007a8afed521cfc13306cbd6747bbc59556e3ca9514c8d94f900fbb56230",
    ),
    (b"test", "928b20366943e2afd11ebc0eae2e53a93bf177a4fcf35bcc64d503704e65e202"),
]
for _ in range(100):
    st = bytes([random.randint(0, 255) for _ in range(1000)])
    with BytesIO(st) as fd:
        ck = file_checksum(fd, algorithm=HashAlgorithm.BLAKE2B_256)
    checksums.append((st, ck))


def test_file_hasher(tmpdir):
    files = []
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        with open(filename, "wb") as f:
            f.write(s)
        files.append(filename)
    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256, use_async=False
    ).check_files(files, pbar_unit="B")
    print(checksum_cache)
    assert len(checksum_cache) == len(checksums)
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        assert filename in checksum_cache
        assert checksum_cache[filename] == c


def test_async_file_hasher(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    files, sizes = [], []
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        with open(filename, "wb") as f:
            f.write(s)
        files.append(filename)
        sizes.append(len(s))
    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256, use_async=True, batch_size=10
    ).check_files(files, pbar_unit="B")
    print(checksum_cache)
    assert len(checksum_cache) == len(checksums)
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        assert filename in checksum_cache
        assert checksum_cache[filename] == c

    files.append(tmpdir / "asdf.bin")
    sizes.append(0)
    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256,
        use_async=True,
        batch_size=10,
        show_progress=False,
    ).check_files(files, pbar_unit="B", file_sizes=sizes)
    print(checksum_cache)
    assert len(checksum_cache) == len(checksums)
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        assert filename in checksum_cache
        assert checksum_cache[filename] == c

    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256, use_async=True, batch_size=6
    ).check_files(files, pbar_unit="it")
    print(checksum_cache)
    assert len(checksum_cache) == len(checksums)
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        assert filename in checksum_cache
        assert checksum_cache[filename] == c

    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256, use_async=False, batch_size=6
    ).check_files(files, pbar_unit="it")
    print(checksum_cache)
    assert len(checksum_cache) == len(checksums)
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        assert filename in checksum_cache
        assert checksum_cache[filename] == c


def test_async_file_hasher_nonexistent_file(tmpdir, caplog):
    files = [tmpdir / "asdf.bin"]
    checksum_cache = AsyncFileHasher(
        algorithm=HashAlgorithm.BLAKE2B_256,
        use_async=True,
        batch_size=10,
    ).check_files(files, pbar_unit="it")
    print([(r.levelname, r) for r in caplog.records])
    print(checksum_cache)
    assert len(checksum_cache) == 0


def test_async_file_hasher_command_available():
    assert AsyncFileHasher.cmd_available("b2sum")
    assert AsyncFileHasher.cmd_available(("b2sum", "-l", "256"))
    assert AsyncFileHasher.cmd_available(("sha256sum",))
    assert not AsyncFileHasher.cmd_available("nonexistent")
    assert not AsyncFileHasher.cmd_available(("sh", "-c", "exit 1"))
