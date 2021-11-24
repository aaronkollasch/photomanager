#!/usr/bin/env python3
import random
import os
from pathlib import Path
import argparse
import time
import json
import math
import string
import datetime
from photomanager.photofile import PhotoFile
from photomanager.database import Database


def generate_test_database(num_uids=10000, r_seed=42):
    random.seed(r_seed, version=2)
    database = Database()
    for i_uid in range(num_uids):
        uid = "".join(random.choices(database.UID_ALPHABET, k=8))
        database.photo_db[uid] = []
        for i_photo in range(random.randint(1, 3)):
            checksum = "".join(random.choices(string.hexdigits, k=64))
            timestamp = random.randint(1037750179000000, 1637750179000000) / 1000000
            dt = datetime.datetime.fromtimestamp(timestamp).astimezone(
                datetime.timezone(datetime.timedelta(hours=random.randint(-12, 12)))
            )
            ts_str = dt.strftime("%Y-%m-%d %H:%M:%S%z")
            img_num = random.randint(0, 9999)
            source_path = f"/path/to/photo/{dt.year}/IMG_{img_num:04d}.JPG"
            store_path = (
                ""
                if random.randint(0, 1)
                else f"{dt.year}/{source_path.rsplit('/', 1)[-1]}"
            )
            filesize = random.randint(100000, 100000000)
            photo = PhotoFile(
                chk=checksum,
                src=source_path,
                ts=timestamp,
                dt=ts_str,
                fsz=filesize,
                sto=store_path,
            )
            database.photo_db[uid].append(photo)
    return database


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bench-dir", type=str, default="/tmp/photomanager_bench_dir")
    parser.add_argument("--num-uids", type=int, default=100000)
    parser.add_argument("--r-seed", type=int, default=42)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-n", "--num-tests", type=int, default=3)
    args = parser.parse_args()

    bench_dir = Path(args.bench_dir)
    os.makedirs(bench_dir, exist_ok=True)

    results = []
    for i_test in range(args.num_tests):
        database = generate_test_database(
            num_uids=args.num_uids, r_seed=args.r_seed + i_test
        )

        time0 = time.perf_counter()
        database.to_file(bench_dir / "db.json", overwrite=True)
        time1 = time.perf_counter()
        database.to_file(bench_dir / "db.json.gz", overwrite=True)
        time2 = time.perf_counter()
        database.to_file(bench_dir / "db.json.zst", overwrite=True)
        time3 = time.perf_counter()
        database.from_file(bench_dir / "db.json")
        time4 = time.perf_counter()
        database.from_file(bench_dir / "db.json.gz")
        time5 = time.perf_counter()
        database.from_file(bench_dir / "db.json.zst")
        time6 = time.perf_counter()

        results.append(
            {
                "num_photos": sum(len(photos) for photos in database.photo_db.values()),
                "raw_save": time1 - time0,
                "gzip_save": time2 - time1,
                "zst_save": time3 - time2,
                "raw_load": time4 - time3,
                "gzip_load": time5 - time4,
                "zst_load": time6 - time5,
                "raw_size": os.path.getsize(bench_dir / "db.json"),
                "gzip_size": os.path.getsize(bench_dir / "db.json.gz"),
                "zstd_size": os.path.getsize(bench_dir / "db.json.zst"),
            }
        )

    summary = {
        k: round(math.fsum(d[k] for d in results) / len(results), 3)
        for k in results[0].keys()
    }
    output = {"args": vars(args), "results": results, "summary": summary}
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
