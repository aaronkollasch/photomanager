from __future__ import annotations
import logging
import os
from io import IOBase
import traceback
import hashlib
from dataclasses import dataclass
import asyncio
from asyncio import subprocess as subprocess_async
import subprocess as subprocess_std
from typing import Union, Optional
from collections.abc import Collection, Iterable
from os import PathLike
import time
from tqdm import tqdm

try:
    from photomanager.database import PhotoManagerBaseException
except ImportError:
    PhotoManagerBaseException = Exception


BLOCK_SIZE = 65536
DEFAULT_HASH_ALGO = "blake2b-256"  # b2sum -l 256
HASH_ALGO_DEFINITIONS = {
    "sha256": {
        "factory": lambda: hashlib.sha256(),
        "command": ("sha256sum",),
    },
    "blake2b-256": {
        "factory": lambda: hashlib.blake2b(digest_size=32),
        "command": ("b2sum", "-l", "256"),
    },
}


class HasherException(PhotoManagerBaseException):
    pass


def file_checksum(
    path: Union[str, PathLike, IOBase], algorithm: str = DEFAULT_HASH_ALGO
) -> str:
    if algorithm in HASH_ALGO_DEFINITIONS:
        hash_obj = HASH_ALGO_DEFINITIONS[algorithm]["factory"]()
    else:
        raise HasherException(f"Hash algorithm not supported: {algorithm}")
    if isinstance(path, IOBase):

        def file_obj():
            return path

    else:

        def file_obj():
            return open(path, "rb")

    with file_obj() as f:
        while block := f.read(BLOCK_SIZE):
            hash_obj.update(block)
    return hash_obj.hexdigest()


@dataclass
class FileHasherJob:
    file_paths: list[bytes]
    pbar_unit: str = "it"
    total_size: Optional[int] = None


class AsyncFileHasher:
    def __init__(
        self,
        algorithm: str = DEFAULT_HASH_ALGO,
        num_workers: int = os.cpu_count(),
        batch_size: int = 50,
        use_async: bool = True,
    ):
        self.algorithm = algorithm
        if algorithm in HASH_ALGO_DEFINITIONS:
            self.command = HASH_ALGO_DEFINITIONS[algorithm]["command"]
        else:
            raise HasherException(f"Hash algorithm not supported: {algorithm}")
        self.use_async = use_async and self.cmd_available(self.command)
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.queue = None
        self.workers = []
        self.output_dict = {}
        self.pbar = None

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

    def __del__(self):
        self.terminate()

    def terminate(self):
        for task in getattr(self, "workers", []):
            task.cancel()
        pbar = getattr(self, "pbar", None)
        if pbar:
            pbar.close()

    async def worker(self):
        while True:
            job = await self.queue.get()
            process = await subprocess_async.create_subprocess_exec(
                *self.command,
                *job.file_paths,
                stdout=subprocess_async.PIPE,
                stderr=subprocess_async.DEVNULL,
            )
            stdout, stderr = await process.communicate()
            try:
                for line in stdout.decode("utf-8").splitlines(keepends=False):
                    if line.strip():
                        checksum, path = line.split(maxsplit=1)
                        self.output_dict[path] = checksum
                if job.pbar_unit == "B":
                    self.pbar.update(job.total_size)
                else:
                    self.pbar.update(len(job.file_paths))
            except (Exception,):
                logging.warning(
                    f"AsyncFileHasher worker encountered an exception!\n"
                    f"hasher command: {self.command}"
                    f"hasher job: {job}\n"
                    f"hasher output: {stdout}\n"
                    f"{traceback.format_exc()}",
                )
            finally:
                self.queue.task_done()

    async def execute_queue(
        self,
        all_jobs: list[FileHasherJob],
        pbar_unit="it",
    ) -> dict[str, str]:
        self.queue = asyncio.Queue()
        self.workers = []
        if pbar_unit == "B":
            for job in all_jobs:
                if job.total_size is None:
                    job.total_size = sum(
                        os.path.getsize(path) for path in job.file_paths
                    )
            self.pbar = tqdm(
                total=sum(job.total_size for job in all_jobs),
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            )
        else:
            self.pbar = tqdm(total=sum(len(job.file_paths) for job in all_jobs))

        # Create worker tasks to process the queue concurrently.
        for i in range(self.num_workers):
            task = asyncio.create_task(self.worker())
            self.workers.append(task)

        for job in all_jobs:
            job.pbar_unit = pbar_unit
            await self.queue.put(job)

        # Wait until the queue is fully processed.
        started_at = time.monotonic()
        await self.queue.join()
        total_time = time.monotonic() - started_at

        # Cancel our worker tasks.
        for task in self.workers:
            task.cancel()
        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers = []
        self.queue = None
        self.pbar.close()
        self.pbar = None

        print(
            f"{self.num_workers} subprocesses worked in parallel for "
            f"{total_time:.2f} seconds"
        )
        return self.output_dict

    @staticmethod
    def make_chunks(it: Iterable, size: int, init: Collection = ()) -> list:
        chunk = list(init)
        for item in it:
            chunk.append(item)
            if len(chunk) - len(init) == size:
                yield chunk
                chunk = list(init)
        if len(chunk) - len(init) > 0:
            yield chunk

    @staticmethod
    def encode(it: Iterable[Union[str, PathLike]]) -> bytes:
        for item in it:
            yield str(item).encode()

    def check_files(
        self,
        file_paths: Iterable[Union[str, PathLike]],
        pbar_unit: str = "it",
        file_sizes: Optional[Iterable[int]] = None,
    ) -> dict[str, str]:
        self.output_dict = {}
        if self.use_async:
            all_jobs = []
            all_paths = list(self.make_chunks(self.encode(file_paths), self.batch_size))
            all_sizes = (
                list(self.make_chunks(file_sizes, self.batch_size))
                if file_sizes is not None
                else None
            )
            for i, paths in enumerate(all_paths):
                job = FileHasherJob(
                    file_paths=paths,
                    pbar_unit=pbar_unit,
                    total_size=sum(all_sizes[i]) if file_sizes is not None else None,
                )
                all_jobs.append(job)
            return asyncio.run(self.execute_queue(all_jobs, pbar_unit=pbar_unit))
        else:
            for path in tqdm(file_paths):
                try:
                    self.output_dict[path] = file_checksum(path, self.algorithm)
                except FileNotFoundError:
                    pass
            return self.output_dict
