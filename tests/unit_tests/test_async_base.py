import logging
import sys
from asyncio import get_event_loop, run, sleep
from dataclasses import dataclass

if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

import pytest

from photomanager.async_base import AsyncJob, AsyncWorkerQueue, make_chunks

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
    """
    Test that chunks have the expected contents
    """
    chunks = list(
        make_chunks(chunks_test["it"], chunks_test["size"], chunks_test["init"])
    )
    print(chunks)
    assert chunks == chunks_test["result"]


@dataclass
class TimeoutJob(AsyncJob):
    index: int = 0
    time: float = 1.0


class AsyncTimeoutWorker(AsyncWorkerQueue):
    def __init__(
        self,
        num_workers: int = 1,
        job_timeout: int | float | None = None,
    ):
        super(AsyncTimeoutWorker, self).__init__(
            num_workers=num_workers,
            show_progress=False,
            job_timeout=job_timeout,
        )

    async def do_job(self, worker_id: int, job: AsyncJob):
        if not isinstance(job, TimeoutJob):
            raise NotImplementedError
        loop = get_event_loop()
        print(job, "started", loop.time(), sep="\t")
        await sleep(job.time)
        self.output_dict[job.index] = job.time
        print(job, "ended  ", loop.time(), sep="\t")


def test_async_execute_queue(caplog):
    caplog.set_level(logging.DEBUG)
    job_timeout = 0.02
    worker = AsyncTimeoutWorker(num_workers=2, job_timeout=job_timeout)
    result_dict = {i: job_timeout * t for i, t in enumerate((0.5, 0.25, 0.75))}
    all_jobs = [TimeoutJob(i, t) for i, t in result_dict.items()]
    result = run(worker.execute_queue(all_jobs), debug=True)
    assert result == result_dict


def test_async_execute_queue_multiple_workers(caplog):
    caplog.set_level(logging.DEBUG)
    job_timeout = 0.02
    worker = AsyncTimeoutWorker(num_workers=8, job_timeout=job_timeout)
    result_dict = {i: job_timeout / 2 for i in range(8)}
    all_jobs = [TimeoutJob(i, t) for i, t in result_dict.items()]

    async def run_timeout():
        try:
            async with timeout(job_timeout):
                print("Execute queue", get_event_loop().time())
                return await worker.execute_queue(all_jobs)
        except TimeoutError:
            print("Timeout at", get_event_loop().time())
            raise
        finally:
            print("Queue finished", get_event_loop().time())

    result = run(run_timeout())
    assert result == result_dict


def test_async_execute_queue_timeout_error(caplog):
    caplog.set_level(logging.DEBUG)
    job_timeout = 0.012
    worker = AsyncTimeoutWorker(num_workers=2, job_timeout=job_timeout)
    result_dict = {i: job_timeout * t for i, t in enumerate((0.9, 0.6, 1.2))}
    all_jobs = [TimeoutJob(i, t) for i, t in result_dict.items()]
    result = run(worker.execute_queue(all_jobs))
    assert any("TimeoutError" in m for m in caplog.messages)
    assert result != result_dict
