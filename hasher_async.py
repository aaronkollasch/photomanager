from __future__ import annotations
import os
import sys
import traceback
import hashlib
import asyncio
from asyncio import subprocess as subprocess_async
import subprocess as subprocess_std
from typing import Union
from collections.abc import Collection, Iterable
from os import PathLike
import time
from tqdm import tqdm
try:
    from database import PhotoManagerBaseException
except ImportError:
    PhotoManagerBaseException = Exception


BLOCK_SIZE = 65536
DEFAULT_HASH_ALGO = 'blake2b-256'  # b2sum -l 256
HASH_ALGO_DEFINITIONS = {
    "sha256": {
        "factory": lambda: hashlib.sha256(),
        "command": ('sha256sum',),
    },
    "blake2b-256": {
        "factory": lambda: hashlib.blake2b(digest_size=32),
        "command": ('b2sum', '-l', '256'),
    },
}


class HasherException(PhotoManagerBaseException):
    pass


def file_checksum(path: Union[str, PathLike], algorithm: str = DEFAULT_HASH_ALGO) -> str:
    if algorithm in HASH_ALGO_DEFINITIONS:
        hash_obj = HASH_ALGO_DEFINITIONS[algorithm]['factory']()
    else:
        raise HasherException(f"Hash algorithm not supported: {algorithm}")
    with open(path, 'rb') as f:
        while block := f.read(BLOCK_SIZE):
            hash_obj.update(block)
    return hash_obj.hexdigest()


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
            self.command = HASH_ALGO_DEFINITIONS[algorithm]['command']
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
            p = subprocess_std.Popen(cmd, stdout=subprocess_std.PIPE, stdin=subprocess_std.PIPE)
            p.communicate()
            if p.returncode:
                return False
            else:
                return True
        except FileNotFoundError:
            return False

    def terminate(self):
        for task in self.workers:
            task.cancel()
        if self.pbar:
            self.pbar.close()

    async def worker(self):
        while True:
            params = await self.queue.get()
            process = await subprocess_async.create_subprocess_exec(
                *self.command, *params,
                stdout=subprocess_async.PIPE,
                stderr=subprocess_async.DEVNULL,
            )
            stdout, stderr = await process.communicate()
            try:
                for line in stdout.decode('utf-8').splitlines(keepends=False):
                    if line.strip():
                        checksum, path = line.split(maxsplit=1)
                        self.output_dict[path] = checksum
                self.pbar.update(n=len(params))
            except (Exception,):
                print(f"AsyncFileHasher worker encountered an exception!\n"
                      f"hasher params: {self.command} {params}\n"
                      f"hasher output: {stdout}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
            finally:
                self.queue.task_done()

    async def execute_queue(self, all_params: list[list[bytes]]) -> dict[str, str]:
        self.queue = asyncio.Queue()
        self.workers = []
        self.pbar = tqdm(total=sum(len(params) for params in all_params))

        # Create worker tasks to process the queue concurrently.
        for i in range(self.num_workers):
            task = asyncio.create_task(self.worker())
            self.workers.append(task)

        for params in all_params:
            await self.queue.put(params)

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

        print(f'{self.num_workers} subprocesses worked in parallel for {total_time:.2f} seconds')
        return self.output_dict

    @staticmethod
    def make_chunks(it: Iterable, size: int, init: Collection = ()) -> list:
        chunk = list(init)
        for item in it:
            chunk.append(item)
            if len(chunk) == size:
                yield chunk
                chunk = list(init)
        if chunk:
            yield chunk

    @staticmethod
    def encode(it: Iterable[str]) -> bytes:
        for item in it:
            yield item.encode()

    def check_files(self, file_paths: Iterable[str]) -> dict[str, str]:
        self.output_dict = {}
        if self.use_async:
            all_params = list(self.make_chunks(self.encode(file_paths), self.batch_size))
            return asyncio.run(self.execute_queue(all_params))
        else:
            for path in tqdm(file_paths):
                try:
                    self.output_dict[path] = file_checksum(path, self.algorithm)
                except FileNotFoundError:
                    pass
            return self.output_dict
