from __future__ import annotations

from os import cpu_count
from typing import Optional, TypeVar
from collections.abc import Collection, Iterable, Generator
from dataclasses import dataclass
import logging
import traceback
import time
from asyncio import Queue, create_task, gather, Task
from tqdm import tqdm


T = TypeVar("T")


def make_chunks(
    it: Iterable[T], size: int, init: Collection[T] = ()
) -> Generator[list[T]]:
    chunk = list(init)
    for item in it:
        chunk.append(item)
        if len(chunk) - len(init) == size:
            yield chunk
            chunk = list(init)
    if len(chunk) - len(init) > 0:
        yield chunk


@dataclass
class AsyncJob:
    @property
    def size(self) -> int:
        raise NotImplementedError


class AsyncWorkerQueue:
    def __init__(
        self,
        num_workers: int = cpu_count(),
        show_progress: bool = False,
    ):
        self.num_workers: int = num_workers
        self.show_progress: bool = show_progress
        self.queue: Optional[Queue[AsyncJob]] = None
        self.workers: list[Task] = []
        self.output_dict: dict = {}
        self.pbar: Optional[tqdm] = None

    def __del__(self):
        self.terminate()

    def terminate(self):
        for task in getattr(self, "workers", []):
            task.cancel()
        pbar: Optional[tqdm] = getattr(self, "pbar", None)
        if pbar:
            pbar.close()

    async def do_job(self, worker_id: int, job: AsyncJob):
        raise NotImplementedError

    def make_pbar(self, all_jobs: Collection[AsyncJob]):
        raise NotImplementedError

    def update_pbar(self, job: AsyncJob):
        raise NotImplementedError

    def close_pbar(self):
        if self.pbar is not None:
            self.pbar.close()

    async def worker(self, worker_id: int):
        try:
            while True:
                job: AsyncJob = await self.queue.get()
                try:
                    await self.do_job(worker_id, job)
                    if self.show_progress:
                        self.update_pbar(job)
                except (Exception,):
                    logging.warning(
                        f"{self.__class__.__name__} worker encountered an exception!\n"
                        f"hasher job: {job}\n"
                        f"{traceback.format_exc()}",
                    )
                finally:
                    self.queue.task_done()
        finally:
            await self.close_worker(worker_id)

    async def close_worker(self, worker_id: int):
        pass

    async def execute_queue(self, all_jobs: Collection[AsyncJob]) -> dict[str, str]:
        """Run jobs"""
        self.queue = Queue()
        self.workers = []
        if self.show_progress:
            self.make_pbar(all_jobs)

        # Create worker tasks to process the queue concurrently.
        num_workers = min(self.num_workers, len(all_jobs))
        for i in range(num_workers):
            task = create_task(self.worker(worker_id=i))
            self.workers.append(task)

        for job in all_jobs:
            await self.queue.put(job)

        # Wait until the queue is fully processed.
        started_at = time.monotonic()
        await self.queue.join()
        total_time = time.monotonic() - started_at

        # Cancel our worker tasks.
        for task in self.workers:
            task.cancel()
        # Wait until all worker tasks are cancelled.
        await gather(*self.workers, return_exceptions=True)

        self.workers = []
        self.queue = None
        self.close_pbar()

        logging.info(
            f"{num_workers} subprocesses worked in parallel for "
            f"{total_time:.2f} seconds"
        )
        return self.output_dict
