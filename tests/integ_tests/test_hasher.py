import logging
import random
from io import BytesIO
from typing import Union, Dict
import pytest
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


@pytest.mark.parametrize(
    "hasher_kwargs",
    [
        dict(algorithm=HashAlgorithm.BLAKE2B_256, use_async=True, batch_size=10),
        dict(
            algorithm=HashAlgorithm.BLAKE2B_256,
            use_async=True,
            batch_size=10,
            show_progress=False,
        ),
        dict(algorithm=HashAlgorithm.BLAKE2B_256, use_async=True, batch_size=6),
        dict(algorithm=HashAlgorithm.BLAKE2B_256, use_async=False, batch_size=6),
    ],
)
@pytest.mark.parametrize(
    "check_kwargs",
    [
        dict(pbar_unit="B", file_sizes=None),
        dict(pbar_unit="B"),
        dict(pbar_unit="it"),
    ],
)
def test_async_file_hasher(
    tmpdir, caplog, hasher_kwargs, check_kwargs: Dict[str, Union[str, list]]
):
    """
    AsyncFileHasher returns the correct checksums and skips nonexistent files
    """
    caplog.set_level(logging.DEBUG)
    files, sizes = [], []
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        with open(filename, "wb") as f:
            f.write(s)
        files.append(filename)
        sizes.append(len(s))
    files.append(tmpdir / "asdf.bin")
    sizes.append(0)
    check_kwargs.setdefault("file_sizes", sizes)
    checksum_cache = AsyncFileHasher(**hasher_kwargs).check_files(files, **check_kwargs)
    print(checksum_cache)
    assert len(checksum_cache) == len(checksums)
    for i, (s, c) in enumerate(checksums):
        filename = tmpdir / f"{i}.bin"
        assert filename in checksum_cache
        assert checksum_cache[filename] == c
    assert (tmpdir / "asdf.bin") not in checksum_cache


@pytest.mark.parametrize(
    "cmd",
    [
        ("b2sum", True),
        (("b2sum", "-l", "256"), True),
        (("sha256sum",), True),
        ("nonexistent", False),
        (("sh", "-c", "exit 1"), False),
    ],
)
def test_async_file_hasher_command_available(cmd):
    """
    AsyncFileHasher.cmd_available returns True for existent hash commands
    and False for nonexistent functions
    """
    assert AsyncFileHasher.cmd_available(cmd[0]) == cmd[1]
