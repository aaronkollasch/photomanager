# -*- coding: utf-8 -*-
# PyExifTool <http://github.com/smarnach/pyexiftool>
# Copyright 2012 Sven Marnach

# This file is part of PyExifTool.
#
# PyExifTool is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the licence, or
# (at your option) any later version, or the BSD licence.
#
# PyExifTool is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING.GPL or COPYING.BSD for more details.

"""
PyExifTool is a Python library to communicate with an instance of Phil
Harvey's excellent ExifTool_ command-line application.  The library
provides the class :py:class:`ExifTool` that runs the command-line
tool in batch mode and features methods to send commands to that
program, including methods to extract meta-information from one or
more image files.  Since ``exiftool`` is run in batch mode, only a
single instance needs to be launched and can be reused for many
queries.  This is much more efficient than launching a separate
process for every single query.

.. _ExifTool: http://www.sno.phy.queensu.ca/~phil/exiftool/

The source code can be checked out from the github repository with

::

    git clone git://github.com/smarnach/pyexiftool.git

Alternatively, you can download a tarball_.  There haven't been any
releases yet.

.. _tarball: https://github.com/smarnach/pyexiftool/tarball/master

PyExifTool is licenced under GNU GPL version 3 or later.

Example usage::

    import exiftool

    files = ["a.jpg", "b.png", "c.tif"]
    with exiftool.ExifTool() as et:
        metadata = et.get_metadata_batch(files)
    for d in metadata:
        print("{:20.20} {:20.20}".format(d["SourceFile"],
                                         d["EXIF:DateTimeOriginal"]))
"""

from __future__ import unicode_literals, annotations
import logging
import os
from collections.abc import Collection, Iterable, Generator
from asyncio import subprocess, run
import orjson
from dataclasses import dataclass, field
from tqdm import tqdm
from .pyexiftool import datetime_tags, best_datetime
from photomanager.async_base import AsyncWorkerQueue, AsyncJob, make_chunks

basestring = (bytes, str)

executable = "exiftool"
"""The name of the executable to run.

If the executable is not located in one of the paths listed in the
``PATH`` environment variable, the full path should be given here.
"""

# Sentinel indicating the end of the output of a sequence of commands.
# The standard value should be fine.
sentinel = b"{ready}"

# The block size when reading from exiftool.  The standard value
# should be fine, though other values might give better performance in
# some cases.
block_size = 4096


@dataclass
class ExifToolJob(AsyncJob):
    mode: str = "default"
    params: Collection[bytes] = field(default_factory=tuple)
    size: int = 1


def make_chunk_jobs(
    filenames: Iterable[str],
    size: int,
    init: Collection[str] = (),
    mode: str = "default",
) -> Generator[ExifToolJob]:
    init = ("-j",) + tuple(init)
    for chunk in make_chunks(filenames, size, init=init):
        yield ExifToolJob(
            mode=mode,
            params=tuple(os.fsencode(p) for p in chunk),
            size=len(chunk) - len(init),
        )


class AsyncExifTool(AsyncWorkerQueue):
    def __init__(
        self,
        executable_=None,
        num_workers=os.cpu_count(),
        show_progress: bool = True,
        batch_size: int = 20,
    ):
        super(AsyncExifTool, self).__init__(
            num_workers=num_workers, show_progress=show_progress
        )
        self.executable = executable if executable_ is None else executable_
        self.running = False
        self.queue = None
        self.batch_size = batch_size
        self.pbar = None
        self.processes: dict[int, subprocess] = {}

    async def do_job(self, worker_id: int, job: ExifToolJob):
        outputs = [b"None"]
        try:
            if worker_id in self.processes:
                process = self.processes[worker_id]
            else:
                process = await subprocess.create_subprocess_exec(
                    self.executable,
                    "-stay_open",
                    "True",
                    "-@",
                    "-",
                    "-common_args",
                    "-G",
                    "-n",
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )
                self.processes[worker_id] = process
            process.stdin.write(b"\n".join(job.params))
            process.stdin.write(b"\n-execute\n")
            await process.stdin.drain()
            outputs = [b""]
            while not outputs[-1][-32:].strip().endswith(sentinel):
                outputs.append(await process.stdout.read(block_size))
            output = b"".join(outputs).strip()[: -len(sentinel)]
            if len(output.strip()) == 0:
                logging.warning(
                    f"exiftool returned an empty string for params {job.params}"
                )
                output = ()
            else:
                output = orjson.loads(output)
            for d in output:
                if "SourceFile" not in d:
                    logging.warning(
                        f"exiftool returned metadata with no SourceFile: {d}"
                    )
                elif job.mode == "best_datetime":
                    self.output_dict[d["SourceFile"]] = best_datetime(d)
                else:
                    self.output_dict[d["SourceFile"]] = d
        except Exception as e:
            print(f"exiftool output: {b''.join(outputs)}\n")
            raise e

    async def close_worker(self, worker_id: int):
        process = self.processes[worker_id]
        await process.communicate(b"-stay_open\nFalse\n")
        del process[worker_id]

    def make_pbar(self, all_jobs: list[ExifToolJob]):
        self.pbar = tqdm(total=sum(job.size for job in all_jobs))

    def update_pbar(self, job: ExifToolJob):
        self.pbar.update(n=job.size)

    def get_metadata_batch(
        self, filenames: Collection[str]
    ) -> dict[str, dict[str, str]]:
        """Return all meta-data for the given files.

        :return: a dictionary of filenames to metadata
        """
        all_jobs = tuple(make_chunk_jobs(filenames, self.batch_size))
        return run(self.execute_queue(all_jobs))

    def get_tags_batch(
        self, tags: Iterable[str], filenames: Collection[str]
    ) -> dict[str, dict[str, str]]:
        """Return only specified tags for the given files.

        The first argument is an iterable of tags.  The tag names may
        include group names, as usual in the format <group>:<tag>.

        The second argument is an iterable of file names.

        :return: a dictionary of filenames to tags
        """
        params = tuple("-" + t for t in tags)
        all_jobs = tuple(make_chunk_jobs(filenames, self.batch_size, init=params))
        return run(self.execute_queue(all_jobs))

    def get_best_datetime_batch(self, filenames: Iterable[str]) -> dict[str, str]:
        params = tuple("-" + t for t in datetime_tags)
        all_jobs = tuple(
            make_chunk_jobs(
                filenames,
                self.batch_size,
                init=params,
                mode="best_datetime",
            )
        )
        return run(self.execute_queue(all_jobs))
