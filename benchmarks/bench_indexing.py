#!/usr/bin/env python3
import random
import os
import sys
from pathlib import Path
import argparse
import time
import json
import shutil
import math
from photomanager.actions.fileops import STORAGE_TYPES
from photomanager.cli import main as cli_main
from photomanager.hasher import DEFAULT_HASH_ALGO, HASH_ALGORITHMS


def randbytes(size):
    if hasattr(random, "randbytes"):
        return random.randbytes(size)
    return bytearray(random.getrandbits(8) for _ in range(size))


def make_test_files(directory, n_folders=20, r_seed=42):
    random.seed(r_seed, version=2)
    # total file size = n_folders * 32 MiB
    for i_folder in range(n_folders):
        c = hex(i_folder)[2:]
        photo_directory = Path(directory) / c
        os.makedirs(photo_directory, exist_ok=True)
        with open(photo_directory / f"{c}_16.jpg", "wb") as f:  # 1 16 MiB "photo"
            f.write(randbytes(2**24))
        for i_photo in range(16):  # 16 1 MiB "photos"
            with open(photo_directory / f"{c}_{i_photo}_1.jpg", "wb") as f:
                f.write(randbytes(2**20))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bench-dir", type=str, default="/tmp/photomanager_bench_dir")
    parser.add_argument("--test-files-dir", type=str, default=None)
    parser.add_argument(
        "--storage-type",
        type=str,
        default="SSD",
        choices=STORAGE_TYPES,
        help="Class of storage medium (HDD, SSD, RAID)",
    )
    parser.add_argument(
        "--hash-algorithm",
        type=str,
        default=DEFAULT_HASH_ALGO.value,
        choices=HASH_ALGORITHMS,
        help=f"Hash algorithm (default={DEFAULT_HASH_ALGO.value})",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-n", "--num-tests", type=int, default=3)
    args = parser.parse_args()

    if not args.test_files_dir:
        test_files_dir = Path(args.bench_dir) / "test_files"
        if not os.path.exists(test_files_dir):
            make_test_files(test_files_dir)
    else:
        test_files_dir = Path(args.test_files_dir)

    results = []
    for i in range(args.num_tests):
        for database_file in Path(args.bench_dir).glob("photos*.db"):
            os.remove(database_file)
        database_file = Path(args.bench_dir) / "photos.db"
        collect_dest_dir = Path(args.bench_dir) / "collect_dest"
        try:
            shutil.rmtree(collect_dest_dir)
        except FileNotFoundError:
            pass

        if not args.verbose:
            sys.stderr = open(os.devnull, "w")
        try:
            cli_main(
                [
                    "create",
                    "--db",
                    database_file,
                    "--hash-algorithm",
                    args.hash_algorithm,
                ]
            )
        except SystemExit:
            pass
        time0 = time.perf_counter()
        try:
            cli_main(
                [
                    "index",
                    "--db",
                    database_file,
                    "--source",
                    test_files_dir,
                    "--storage-type",
                    args.storage_type,
                ]
            )
        except SystemExit:
            pass
        time1 = time.perf_counter()
        try:
            cli_main(
                ["collect", "--db", database_file, "--destination", collect_dest_dir]
            )
        except SystemExit:
            pass
        time2 = time.perf_counter()
        try:
            cli_main(
                [
                    "verify",
                    "--db",
                    database_file,
                    "--destination",
                    collect_dest_dir,
                    "--storage-type",
                    args.storage_type,
                ]
            )
        except SystemExit:
            pass
        time3 = time.perf_counter()
        if not args.verbose:
            sys.stderr.close()
            sys.stderr = sys.__stderr__
        results.append(
            {"index": time1 - time0, "collect": time2 - time1, "verify": time3 - time2}
        )

    summary = {
        k: round(math.fsum(d[k] for d in results) / len(results), 3)
        for k in results[0].keys()
    }
    output = {"args": vars(args), "results": results, "summary": summary}
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
