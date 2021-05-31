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

from __future__ import unicode_literals

import logging
import os
import asyncio
from asyncio import subprocess
import orjson
import time
import traceback
from tqdm import tqdm
from photomanager.pyexiftool import datetime_tags, best_datetime

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


class AsyncExifTool(object):
    def __init__(self, executable_=None, num_workers=os.cpu_count(), batch_size=20):
        if executable_ is None:
            self.executable = executable
        else:
            self.executable = executable_
        self.running = False
        self.output_dict = {}
        self.queue = None
        self.workers: list[asyncio.Task] = []
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.pbar = None

    def terminate(self):
        for task in self.workers:
            task.cancel()
        if self.pbar:
            self.pbar.close()

    def __del__(self):
        self.terminate()

    async def worker(self, mode=None):
        process = None
        try:
            while True:
                params = await self.queue.get()
                if process is None:
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
                process.stdin.write(b"\n".join(params + (b"-execute\n",)))
                await process.stdin.drain()
                outputs = [b""]
                while not outputs[-1][-32:].strip().endswith(sentinel):
                    outputs.append(await process.stdout.read(block_size))
                try:
                    output = b"".join(outputs).strip()[: -len(sentinel)]
                    output = orjson.loads(output)
                    for d in output:
                        if mode == "best_datetime":
                            self.output_dict[d["SourceFile"]] = best_datetime(d)
                        else:
                            self.output_dict[d["SourceFile"]] = d
                    self.pbar.update(n=len(output))
                except (Exception,):
                    logging.warning(
                        f"AsyncExifTool worker encountered an exception!\n"
                        f"exiftool params: {self.executable} {params}\n"
                        f"exiftool output: {b''.join(outputs)}\n"
                        f"{traceback.format_exc()}",
                    )
                finally:
                    self.queue.task_done()
        finally:
            if process is not None:
                await process.communicate(b"-stay_open\nFalse\n")

    async def execute_json(self, *params):
        params = map(os.fsencode, params)
        await self.queue.put((b"-j", *params))

    async def execute_queue(self, all_params, num_files, mode=None):
        self.output_dict = {}
        self.queue = asyncio.Queue()
        self.pbar = tqdm(total=num_files)

        # Create worker tasks to process the queue concurrently.
        for i in range(self.num_workers):
            task = asyncio.create_task(self.worker(mode=mode))
            self.workers.append(task)

        for params in all_params:
            await self.execute_json(*params)

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
            f"{self.num_workers} subprocesses worked in "
            f"parallel for {total_time:.2f} seconds"
        )
        return self.output_dict

    @staticmethod
    def make_chunks(it, size, init=()):
        chunk = list(init)
        for item in it:
            chunk.append(item)
            if len(chunk) - len(init) == size:
                yield chunk
                chunk = list(init)
        if len(chunk) - len(init) > 0:
            yield chunk

    def get_metadata_batch(self, filenames):
        """Return all meta-data for the given files.

        The return value will have the format described in the
        documentation of :py:meth:`execute_json()`.
        """
        all_params = list(self.make_chunks(filenames, self.batch_size))
        return asyncio.run(self.execute_queue(all_params, len(filenames)))

    def get_tags_batch(self, tags, filenames):
        """Return only specified tags for the given files.

        The first argument is an iterable of tags.  The tag names may
        include group names, as usual in the format <group>:<tag>.

        The second argument is an iterable of file names.

        The format of the return value is the same as for
        :py:meth:`execute_json()`.
        """
        params = tuple("-" + t for t in tags)
        all_params = list(self.make_chunks(filenames, self.batch_size, init=params))
        return asyncio.run(self.execute_queue(all_params, len(filenames)))

    def get_best_datetime_batch(self, filenames):
        params = tuple("-" + t for t in datetime_tags)
        all_params = list(self.make_chunks(filenames, self.batch_size, init=params))
        return asyncio.run(
            self.execute_queue(all_params, len(filenames), mode="best_datetime")
        )
