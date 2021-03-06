from __future__ import annotations

from enum import Enum
from io import IOBase
import hashlib
from dataclasses import dataclass, field
from asyncio import run
from asyncio import subprocess as subprocess_async
import subprocess as subprocess_std
from typing import Union, Optional, Generator
from collections.abc import Iterable
from os import PathLike, cpu_count, fsencode
from os.path import getsize
from tqdm import tqdm
from photomanager import PhotoManagerBaseException
from photomanager.async_base import AsyncWorkerQueue, AsyncJob, make_chunks


BLOCK_SIZE = 65536


class HashAlgorithm(Enum):
    SHA256 = "sha256"
    BLAKE2B_256 = "blake2b-256"


DEFAULT_HASH_ALGO = HashAlgorithm.BLAKE2B_256  # b2sum -l 256
HASH_ALGO_DEFINITIONS = {
    HashAlgorithm.SHA256: {
        "factory": lambda: hashlib.sha256(),
        "command": ("sha256sum",),
    },
    HashAlgorithm.BLAKE2B_256: {
        "factory": lambda: hashlib.blake2b(digest_size=32),
        "command": ("b2sum", "-l", "256"),
    },
}


class HasherException(PhotoManagerBaseException):
    pass


def _update_hash_obj(hash_obj, fd):
    while block := fd.read(BLOCK_SIZE):
        hash_obj.update(block)


def file_checksum(
    file: Union[bytes, str, PathLike, IOBase],
    algorithm: HashAlgorithm = DEFAULT_HASH_ALGO,
) -> str:
    if algorithm in HASH_ALGO_DEFINITIONS:
        hash_obj = HASH_ALGO_DEFINITIONS[algorithm]["factory"]()
    else:
        raise HasherException(f"Hash algorithm not supported: {algorithm}")
    if isinstance(file, IOBase):
        _update_hash_obj(hash_obj, file)
    else:
        with open(file, "rb") as f:
            _update_hash_obj(hash_obj, f)
    return hash_obj.hexdigest()


def check_files(
    file_paths: Iterable[Union[bytes, str, PathLike, IOBase]],
    algorithm: HashAlgorithm = DEFAULT_HASH_ALGO,
) -> dict[str, str]:
    output_dict = {}
    for path in tqdm(file_paths):
        try:
            output_dict[path] = file_checksum(path, algorithm)
        except FileNotFoundError:
            pass
    return output_dict


@dataclass
class FileHasherJob(AsyncJob):
    file_paths: list[bytes] = field(default_factory=list)
    known_total_size: Optional[int] = None

    @property
    def size(self) -> int:
        if self.known_total_size is None:
            self.known_total_size = sum(getsize(path) for path in self.file_paths)
        return self.known_total_size


class AsyncFileHasher(AsyncWorkerQueue):
    def __init__(
        self,
        num_workers: int = cpu_count(),
        show_progress: bool = True,
        batch_size: int = 50,
        algorithm: HashAlgorithm = DEFAULT_HASH_ALGO,
        use_async: bool = True,
    ):
        super(AsyncFileHasher, self).__init__(
            num_workers=num_workers, show_progress=show_progress
        )
        self.batch_size: int = batch_size
        self.algorithm = algorithm
        if algorithm in HASH_ALGO_DEFINITIONS:
            self.command = HASH_ALGO_DEFINITIONS[algorithm]["command"]
        else:
            raise HasherException(f"Hash algorithm not supported: {algorithm}")
        self.use_async = use_async and self.cmd_available(self.command)
        self.pbar_unit: str = "it"

    @staticmethod
    def cmd_available(cmd) -> bool:
        try:
            p = subprocess_std.Popen(
                cmd, stdout=subprocess_std.PIPE, stdin=subprocess_std.PIPE
            )
            p.communicate()
            if p.returncode:
                return False
            else:
                return True
        except FileNotFoundError:
            return False

    async def do_job(self, worker_id: int, job: FileHasherJob):
        stdout = None
        try:
            process = await subprocess_async.create_subprocess_exec(
                *self.command,
                *job.file_paths,
                stdout=subprocess_async.PIPE,
                stderr=subprocess_async.DEVNULL,
            )
            stdout, stderr = await process.communicate()
            for line in stdout.decode("utf-8").splitlines(keepends=False):
                if line.strip():
                    checksum, path = line.split(maxsplit=1)
                    self.output_dict[path] = checksum
        except Exception as e:
            print("hasher output:", stdout)
            raise e

    def make_pbar(self, all_jobs: list[FileHasherJob]):
        if self.pbar_unit == "B":
            self.pbar = tqdm(
                total=sum(job.size for job in all_jobs),
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            )
        else:
            self.pbar = tqdm(total=sum(len(job.file_paths) for job in all_jobs))

    def update_pbar(self, job: FileHasherJob):
        if self.pbar_unit == "B":
            self.pbar.update(job.size)
        else:
            self.pbar.update(len(job.file_paths))

    @staticmethod
    def encode(it: Iterable[Union[str, PathLike]]) -> Generator[bytes]:
        for item in it:
            yield fsencode(item)

    def check_files(
        self,
        file_paths: Iterable[Union[str, PathLike]],
        pbar_unit: str = "it",
        file_sizes: Optional[Iterable[int]] = None,
    ) -> dict[str, str]:
        if not self.use_async:
            return check_files(file_paths=file_paths, algorithm=self.algorithm)

        self.output_dict = {}
        self.pbar_unit = pbar_unit
        all_jobs = []
        all_paths = list(make_chunks(self.encode(file_paths), self.batch_size))
        all_sizes = (
            list(make_chunks(file_sizes, self.batch_size))
            if file_sizes is not None
            else None
        )
        for i, paths in enumerate(all_paths):
            job = FileHasherJob(
                file_paths=paths,
                known_total_size=sum(all_sizes[i]) if file_sizes is not None else None,
            )
            all_jobs.append(job)
        return run(self.execute_queue(all_jobs))
