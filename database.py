import os
from os import PathLike
import stat
from math import log
from uuid import uuid4
from pathlib import Path
from datetime import datetime
import shutil
import dataclasses
import json
import hashlib
import logging
import traceback
import asyncio
from asyncio import subprocess
from subprocess import Popen, DEVNULL
import time
from typing import Union, Optional, Type, TypeVar
from collections.abc import Collection, Iterable
from pyexiftool import ExifTool
from pyexiftool_async import AsyncExifTool
from tqdm import tqdm

BLOCK_SIZE = 65536
DEFAULT_HASH_ALGO = 'blake2b-256'  # b2sum -l 256
PF = TypeVar('PF', bound='PhotoFile')


@dataclasses.dataclass
class PhotoFile:
    checksum: str  # SHA-256 checksum of photo file
    source_path: str  # Absolute path where photo was found
    datetime: str  # Datetime string for best estimated creation date
    timestamp: float  # POSIX timestamp of best estimated creation date
    file_size: int  # Photo file size
    store_path: str = ''  # Relative path where photo is stored, if it is stored
    priority: int = 10  # Photo priority (lower is preferred)

    @classmethod
    def from_file(
            cls: Type[PF],
            source_path: Union[str, PathLike],
            algorithm: str = DEFAULT_HASH_ALGO,
            priority: int = 10
    ) -> PF:
        photo_hash: str = file_checksum(source_path, algorithm)
        dt = get_media_datetime(source_path)
        timestamp = datetime_str_to_object(dt).timestamp()
        file_size = os.path.getsize(source_path)
        return cls(
            checksum=photo_hash,
            source_path=str(source_path),
            datetime=dt,
            timestamp=timestamp,
            file_size=file_size,
            store_path='',
            priority=priority,
        )

    @classmethod
    def from_file_cached(
            cls: Type[PF],
            source_path: str,
            checksum_cache: dict[str, str],
            datetime_cache: dict[str, str],
            algorithm: str = DEFAULT_HASH_ALGO,
            priority: int = 10,
    ) -> PF:
        photo_hash: str = (
            checksum_cache[source_path] if source_path in checksum_cache
            else file_checksum(source_path, algorithm)
        )
        dt = (
            datetime_cache[source_path] if source_path in datetime_cache
            else get_media_datetime(source_path)
        )
        timestamp = datetime_str_to_object(dt).timestamp()
        file_size = os.path.getsize(source_path)
        return cls(
            checksum=photo_hash,
            source_path=str(source_path),
            datetime=dt,
            timestamp=timestamp,
            file_size=file_size,
            store_path='',
            priority=priority,
        )


def pf_from_dict(d: dict) -> PhotoFile:
    return PhotoFile(**d)


def pf_to_dict(pf: PhotoFile) -> dict:
    return dataclasses.asdict(pf)


class EnhancedJSONEncoder(json.JSONEncoder):
    """Encodes dataclasses as dictionaries"""
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def file_checksum(path: Union[str, PathLike], algorithm: str = DEFAULT_HASH_ALGO) -> str:
    if algorithm == 'sha256':
        hash_obj = hashlib.sha256()
    elif algorithm == 'blake2b-256':
        hash_obj = hashlib.blake2b(digest_size=32)
    else:
        raise PhotoManagerException(f"Hash algorithm not supported: {algorithm}")
    with open(path, 'rb') as f:
        while block := f.read(BLOCK_SIZE):
            hash_obj.update(block)
    return hash_obj.hexdigest()


class AsyncFileHasher:
    def __init__(
            self, algorithm: str = DEFAULT_HASH_ALGO,
            num_workers: int = os.cpu_count(), batch_size: int = 20, use_async: bool = True
    ):
        self.algorithm = algorithm
        if self.algorithm == 'blake2b-256':
            self.command = ('b2sum', '-l', '256')
        elif self.algorithm == 'sha256sum':
            self.command = ('sha256sum',)
        else:
            raise PhotoManagerException(f"Hash algorithm not supported: {algorithm}")
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
            p = Popen(cmd, stdout=DEVNULL)
            p.terminate()
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
            process = await subprocess.create_subprocess_exec(
                *self.command,
                *params,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL)
            stdout, stderr = await process.communicate()
            for line in stdout.decode('utf-8').splitlines(keepends=False):
                if line.strip():
                    checksum, path = line.split(maxsplit=1)
                    self.output_dict[path] = checksum
            self.pbar.update(n=len(params))
            self.queue.task_done()

    async def execute_queue(self, all_params: list[list[bytes]]) -> dict[str, str]:
        self.output_dict = {}
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
        if self.use_async:
            all_params = list(self.make_chunks(self.encode(file_paths), self.batch_size))
            return asyncio.run(self.execute_queue(all_params))
        else:
            for path in tqdm(file_paths):
                self.output_dict[path] = file_checksum(path, self.algorithm)
            return self.output_dict


def datetime_str_to_object(ts_str: str) -> datetime:
    """Parses a datetime string into a datetime object"""
    if '.' in ts_str:
        for fmt in ('%Y:%m:%d %H:%M:%S.%f%z', '%Y:%m:%d %H:%M:%S.%f'):
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                pass
    else:
        for fmt in ('%Y:%m:%d %H:%M:%S%z', '%Y:%m:%d %H:%M:%S', '%Y:%m:%d %H:%M%z', '%Y:%m:%d %H:%M'):
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                pass
    raise ValueError(f"Could not parse datetime str: {repr(ts_str)}")


def get_media_datetime(path: Union[str, PathLike]) -> str:
    """Gets the best known datetime string for a file"""
    return ExifTool().get_best_datetime(path)


unit_list = list(zip(['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'], [0, 0, 1, 2, 2, 2]))


def sizeof_fmt(num: int) -> str:
    """Human friendly file size
    https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size"""
    if num > 1:
        exponent = min(int(log(num, 1024)), len(unit_list) - 1)
        quotient = float(num) / 1024**exponent
        unit, num_decimals = unit_list[exponent]
        format_string = '{:.%sf} {}' % num_decimals
        return format_string.format(quotient, unit)
    if num == 0:
        return '0 bytes'
    if num == 1:
        return '1 byte'


class PhotoManagerBaseException(Exception):
    pass


class DatabaseException(PhotoManagerBaseException):
    pass


class PhotoManagerException(PhotoManagerBaseException):
    pass


DB = TypeVar('DB', bound='Database')


class Database:
    def __init__(self):
        self.db: dict = {'photo_db': {}, 'command_history': {}, 'hash_algorithm': DEFAULT_HASH_ALGO}
        self.photo_db: dict[str, list[PhotoFile]] = self.db['photo_db']
        self.hash_to_uid: dict[str, str] = {}
        self.timestamp_to_uids: dict[float, dict[str, None]] = {}
        self.hash_algorithm = DEFAULT_HASH_ALGO

    @classmethod
    def from_file(cls: Type[DB], path: Union[str, PathLike]) -> DB:
        """Loads a Database from a path"""
        db = cls()
        if os.path.exists(path):
            with open(path) as f:
                db.db = json.load(f)
                db.photo_db = db.db['photo_db']
                for uid in db.photo_db.keys():
                    db.photo_db[uid] = [pf_from_dict(d) for d in db.photo_db[uid]]
                db.db.setdefault('hash_algorithm', 'sha256')  # legacy dbs do not specify algo and use sha256
                db.hash_algorithm = db.db['hash_algorithm']
            for uid, photos in db.photo_db.items():
                for photo in photos:
                    db.hash_to_uid[photo.checksum] = uid
                    if photo.timestamp in db.timestamp_to_uids:
                        db.timestamp_to_uids[photo.timestamp][uid] = None
                    else:
                        db.timestamp_to_uids[photo.timestamp] = {uid: None}
        return db

    def to_file(self, path: Union[str, PathLike]) -> None:
        """Saves the db to path and moves an existing database at that path to a different location"""
        path = Path(path)
        if path.is_file():
            new_path = path.with_stem(
                f"{path.stem}_"
                f"{datetime.fromtimestamp(path.stat().st_mtime).strftime('%Y-%m-%d_%H-%M-%S')}"
            )
            if not new_path.exists():
                shutil.move(path, new_path)
        with open(path, 'w') as f:
            json.dump(self.db, fp=f, cls=EnhancedJSONEncoder)

    def find_photo(self, photo: PhotoFile) -> Optional[str]:
        """Finds a photo in the database and returns its uid

        Matches first by file checksum, then by timestamp+filename (case-insensitive).

        :return the photo's uid, or None if not found"""
        if photo.checksum in self.hash_to_uid:
            return self.hash_to_uid[photo.checksum]
        uids = self.timestamp_to_uids.get(photo.timestamp, None)
        if uids:
            name_matches = []
            photo_name = Path(photo.source_path).name
            for uid in uids:
                if any(photo_name.lower() == Path(pf.source_path).name.lower() for pf in self.photo_db[uid]):
                    name_matches.append(uid)
            if name_matches:
                if len(name_matches) > 1:
                    print(f"ambiguous timestamp+name match: {photo.source_path}: {name_matches}")
                return name_matches[0]
            else:
                return None
        else:
            return None

    def add_photo(self, photo: PhotoFile, uid: Optional[str]) -> Optional[str]:
        """Adds a photo into the database with specified uid (can be None)

        Skips and returns None if the photo checksum is already in the database under a different uid
        or the photo checksum+source_path pair is already in the database.

        If uid is None, a new random uid will be generated.

        :return the added photo's uid, or None if not added"""

        if photo.checksum in self.hash_to_uid:
            if uid is not None and uid != self.hash_to_uid[photo.checksum]:
                return None
            if any(photo.checksum == pf.checksum and photo.source_path == pf.source_path
                   for pf in self.photo_db[self.hash_to_uid[photo.checksum]]):
                return None
        if uid is None:
            uid = self.hash_to_uid.get(photo.checksum, uuid4().hex)
        photos = self.photo_db.get(uid, None)
        if photos is not None:
            if photo.source_path not in (p.source_path for p in photos):
                photos.append(photo)
                photos.sort(key=lambda pf: pf.priority)
        else:
            self.photo_db[uid] = [photo]
        self.hash_to_uid[photo.checksum] = uid
        if photo.timestamp in self.timestamp_to_uids:
            self.timestamp_to_uids[photo.timestamp][uid] = None
        else:
            self.timestamp_to_uids[photo.timestamp] = {uid: None}
        return uid

    def import_photos(
            self,
            files: Collection[Union[str, PathLike]],
            priority: int = 10,
            storage_type: str = 'HDD'
    ) -> int:
        """Imports photo files into the database with a designated priority

        :param files: the photo file paths to import
        :param priority: the imported photos' priority
        :param storage_type: the type of media importing from (uses more async if SSD)
        :return: the number of photos imported"""
        logger = logging.getLogger()
        num_added_photos = num_merged_photos = num_skipped_photos = num_error_photos = 0
        if storage_type == 'SSD':
            async_hashes = True
            async_exif = os.cpu_count()
        else:
            async_hashes = False  # concurrent reads of sequential files can lead to thrashing
            async_exif = 4  # exiftool is partially CPU-bound and benefits from async
        print("Collecting media hashes")
        checksum_cache = AsyncFileHasher(algorithm=self.hash_algorithm, use_async=async_hashes).check_files(files)
        print("Collecting media dates and times")
        datetime_cache = AsyncExifTool(num_workers=async_exif).get_best_datetime_batch(files)
        for current_file in tqdm(files):
            if logger.isEnabledFor(logging.DEBUG):
                tqdm.write(f"Importing {current_file}")
            try:
                pf = PhotoFile.from_file_cached(
                    current_file,
                    checksum_cache=checksum_cache,
                    datetime_cache=datetime_cache,
                    algorithm=self.hash_algorithm, priority=priority
                )
                uid = self.find_photo(photo=pf)
                result = self.add_photo(photo=pf, uid=uid)

                if result is None:
                    num_skipped_photos += 1
                elif uid is None:
                    num_added_photos += 1
                else:
                    num_merged_photos += 1
            except Exception as e:
                print(f"Error importing {current_file}")
                tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
                print(tb_str)
                num_error_photos += 1

        print(f"Imported {num_added_photos+num_merged_photos}/{len(files)} items")
        print(f"Added {num_added_photos} new items and merged {num_merged_photos} items")
        if num_skipped_photos or num_error_photos:
            print(f"Skipped {num_skipped_photos} items and errored on {num_error_photos} items")
        return num_added_photos + num_merged_photos

    def get_chosen_photos(self) -> list[PhotoFile]:
        """Gets all photos this database has stored or would choose to store"""
        chosen_photos = []
        for uid, photos in self.photo_db.items():
            highest_priority = min(photo.priority for photo in photos)
            new_chosen_photos = list(photo for photo in photos if photo.store_path)
            stored_checksums = set(photo.checksum for photo in new_chosen_photos)
            for photo in photos:
                if photo.priority == highest_priority and photo.checksum not in stored_checksums:
                    new_chosen_photos.append(photo)
                    stored_checksums.add(photo.checksum)
            chosen_photos.extend(new_chosen_photos)
        return chosen_photos

    def collect_to_directory(self, directory: Union[str, PathLike]) -> int:
        """Collects photos in the database into a directory

        Collects only photos that have a store_path set or that have the highest priority
        and whose checksum does not match any stored alternative photo

        Updates the store_path for photos newly stored. Store_paths are relative to the storage directory.
        Stored photos have permissions set to read-only for all.

        :param directory the photo storage directory
        :return the number of photos collected"""

        print("Collecting photos.")
        estimated_library_size = sum(photo.file_size for photo in self.get_chosen_photos())
        print(f"Estimated total library size: {sizeof_fmt(estimated_library_size)}")
        directory = Path(directory).expanduser().resolve()
        num_transferred_photos = num_added_photos = num_missed_photos = num_stored_photos = 0
        logger = logging.getLogger()
        for uid, photos in tqdm(self.photo_db.items()):
            highest_priority = min(photo.priority for photo in photos)
            stored_checksums = set()
            photos_marked_as_stored = [photo for photo in photos if photo.store_path]
            highest_priority_photos = [photo for photo in photos if photo.priority == highest_priority
                                       and not photo.store_path]
            for photo in photos_marked_as_stored:
                abs_store_path = directory / photo.store_path
                if abs_store_path.exists():
                    stored_checksums.add(photo.checksum)
                    num_stored_photos += 1
                elif os.path.exists(photo.source_path):
                    if logger.isEnabledFor(logging.DEBUG):
                        tqdm.write(f"Copying {photo.source_path} to {abs_store_path}")
                    os.makedirs(abs_store_path.parent, exist_ok=True)
                    shutil.copy2(photo.source_path, abs_store_path)
                    os.chmod(abs_store_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                    stored_checksums.add(photo.checksum)
                    num_transferred_photos += 1
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        tqdm.write(f"Photo not found: {photo.source_path}")
                    num_missed_photos += 1
            for photo in highest_priority_photos:
                if photo.checksum in stored_checksums:
                    continue
                photo_datetime = datetime_str_to_object(photo.datetime)
                rel_store_path = (
                    f"{photo_datetime.strftime('%Y/%m-%b/%Y-%m-%d_%H-%M-%S')}-"
                    f"{photo.checksum[:7]}-"
                    f"{Path(photo.source_path).name}"
                )
                abs_store_path = directory / rel_store_path
                if abs_store_path.exists():
                    if logger.isEnabledFor(logging.DEBUG):
                        tqdm.write(f"Photo already present: {abs_store_path}")
                    photo.store_path = rel_store_path
                    stored_checksums.add(photo.checksum)
                    num_stored_photos += 1
                elif os.path.exists(photo.source_path):
                    if logger.isEnabledFor(logging.DEBUG):
                        tqdm.write(f"Copying {photo.source_path} to {abs_store_path}")
                    os.makedirs(abs_store_path.parent, exist_ok=True)
                    shutil.copy2(photo.source_path, abs_store_path)
                    os.chmod(abs_store_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                    photo.store_path = rel_store_path
                    stored_checksums.add(photo.checksum)
                    num_added_photos += 1
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        tqdm.write(f"Photo not found: {photo.source_path}")
                    num_missed_photos += 1

        print(f"Copied {num_added_photos+num_transferred_photos} items: "
              f"{num_added_photos} new items and {num_transferred_photos} items marked as stored elsewhere")
        if num_stored_photos or num_missed_photos:
            print(f"Skipped {num_stored_photos} items already stored and {num_missed_photos} missing items")
        return num_added_photos + num_transferred_photos

    def clean_stored_photos(
            self,
            directory: Union[str, PathLike],
            subdirectory: Union[str, PathLike] = '',
            dry_run: bool = False
    ) -> int:
        """Removes lower-priority stored photos if a higher-priority version is stored

        :param directory the photo storage directory
        :param subdirectory remove only photos within subdirectory
        :param dry_run if True, do not remove photos
        :return the number of photos removed"""
        num_removed_photos = num_missing_photos = total_file_size = 0
        directory = Path(directory).expanduser().resolve()
        subdirectory = Path(subdirectory)
        if subdirectory.is_absolute():
            raise DatabaseException("Absolute subdirectory not supported")
        abs_subdirectory = directory / subdirectory
        photos_to_remove = []
        for photos in self.photo_db.values():
            highest_stored_priority = min(
                photo.priority for photo in photos
                if photo.store_path and (directory / photo.store_path).exists()
            )
            for photo in photos:
                abs_store_path = directory / photo.store_path
                if (photo.priority > highest_stored_priority and photo.store_path and
                        abs_store_path.is_relative_to(abs_subdirectory)):
                    photos_to_remove.append(photo)
                    total_file_size += photo.file_size
        print(f"Identified {len(photos_to_remove)} lower-priority items for removal")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        logger = logging.getLogger()
        for photo in tqdm(photos_to_remove):
            abs_store_path = directory / photo.store_path
            if abs_store_path.exists():
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(f"{'Will remove' if dry_run else 'Removing'}: {abs_store_path}")
                if not dry_run:
                    os.remove(abs_store_path)
                    photo.store_path = ''
                num_removed_photos += 1
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
        print(f"{'Found' if dry_run else 'Removed'} {num_removed_photos} items "
              f"and skipped {num_missing_photos} missing items")
        return num_removed_photos

    def verify_stored_photos(
            self,
            directory: Union[str, PathLike],
            subdirectory: Union[str, PathLike] = '',
            storage_type: str = 'HDD',
    ) -> int:
        """Check the files stored in directory against checksums in the database

        :param directory: the photo storage directory
        :param subdirectory: verify only photos within subdirectory
        :param storage_type: the type of media importing from (uses async if SSD)
        :return: the number of errors found"""
        num_correct_photos = num_incorrect_photos = num_missing_photos = total_file_size = 0
        directory = Path(directory).expanduser().resolve()
        subdirectory = Path(subdirectory)
        if subdirectory.is_absolute():
            raise DatabaseException("Absolute subdirectory not supported")
        abs_subdirectory = directory / subdirectory
        stored_photos = []
        files = []
        for photos in self.photo_db.values():
            for photo in photos:
                abs_store_path = directory / photo.store_path
                if photo.store_path and abs_store_path.is_relative_to(abs_subdirectory):
                    stored_photos.append(photo)
                    total_file_size += photo.file_size
                    if abs_store_path.exists():
                        files.append(str(abs_store_path))
        print(f"Verifying {len(stored_photos)} items")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        if storage_type == 'SSD':
            async_hashes = True
        else:
            async_hashes = False  # concurrent reads of sequential files can lead to thrashing
        print("Collecting media hashes")
        checksum_cache = AsyncFileHasher(algorithm=self.hash_algorithm, use_async=async_hashes).check_files(files)
        for photo in tqdm(stored_photos):
            abs_store_path = directory / photo.store_path
            if not abs_store_path.exists():
                tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
            elif (
                    checksum_cache[str(abs_store_path)] if str(abs_store_path) in checksum_cache
                    else file_checksum(abs_store_path, self.hash_algorithm)
                    == photo.checksum
            ):
                num_correct_photos += 1
            else:
                tqdm.write(f"Incorrect checksum: {abs_store_path}")
                num_incorrect_photos += 1

        print(f"Checked {num_correct_photos+num_incorrect_photos+num_missing_photos} items")
        if num_incorrect_photos or num_missing_photos:
            print(f"Found {num_incorrect_photos} incorrect and {num_missing_photos} missing items")
        else:
            print("No errors found")
        return num_incorrect_photos + num_missing_photos

    def get_stats(self) -> tuple[int, int, int, int]:
        """Get database item statistics

        :return num_uids, num_photos, num_stored_photos, total_file_size"""
        num_uids = num_photos = num_stored_photos = total_file_size = 0
        for photos in self.photo_db.values():
            num_uids += 1
            for photo in photos:
                num_photos += 1
                if photo.store_path:
                    num_stored_photos += 1
                    total_file_size += photo.file_size
        print(f"Total items:        {num_photos}")
        print(f"Total unique items: {num_uids}")
        print(f"Total stored items: {num_stored_photos}")
        print(f"Total file size:    {sizeof_fmt(total_file_size)}")
        return num_uids, num_photos, num_stored_photos, total_file_size

    def make_hash_map(self, new_algo: str, hash_map: Optional[dict[str, str]] = None) -> dict[str, str]:
        """Make a map of file checksums in order to migrate hashing algorithms

        Checks source file hashes using the old algorithm to make sure the new hashes are correct.
        If the source has an incorrect hash, does not map checksum and instead denotes the hash by
        appending ':{algorithm}'

        :param new_algo the new algorithm to use
        :param hash_map the map from old hashes to new hashes; will be updated with new mappings
        :return the hash map"""
        if hash_map is None:
            hash_map = {}
        old_algo = self.hash_algorithm
        print(f"Converting {old_algo} to {new_algo}")
        num_correct_photos = num_incorrect_photos = num_missing_photos = num_skipped_photos = 0
        all_photos = [photo for photos in self.photo_db.values() for photo in photos]
        for photo in tqdm(all_photos):
            if photo.checksum in hash_map:
                num_skipped_photos += 1
            elif os.path.exists(photo.source_path):
                if photo.checksum == file_checksum(photo.source_path, old_algo):
                    hash_map[photo.checksum] = file_checksum(photo.source_path, new_algo)
                    num_correct_photos += 1
                else:
                    tqdm.write(f"Incorrect checksum: {photo.source_path}")
                    hash_map[photo.checksum] = f"{photo.checksum}:{old_algo}"
                    num_incorrect_photos += 1
            else:
                num_missing_photos += 1

        print(f"Mapped {num_correct_photos} items")
        if num_skipped_photos:
            print(f"Skipped {num_skipped_photos} items")
        if num_incorrect_photos or num_missing_photos:
            print(f"Found {num_incorrect_photos} incorrect and {num_missing_photos} missing items")
        return hash_map

    def map_hashes(self, new_algo: str, hash_map: dict[str, str], map_all: bool = False) -> Optional[int]:
        """Map the database's checksums to a new algorithm

        If a checksum cannot be mapped, it is appended by ':{algorithm}'
        to denote that it was made with a different algorithm

        :param new_algo the hashing algorithm used to make hash_map
        :param hash_map the map from old hashes to new hashes
        :param map_all set to True to make sure that all hashes can be mapped
        :return the number of hashes not mapped"""
        num_correct_photos = num_skipped_photos = 0
        old_algo = self.hash_algorithm
        all_photos = [photo for photos in self.photo_db.values() for photo in photos]
        if map_all and (num_skipped_photos := sum(photo.checksum not in hash_map for photo in all_photos)):
            print(f"Not all items will be mapped: {num_skipped_photos}")
            return None
        for photo in tqdm(all_photos):
            if photo.checksum in hash_map:
                photo.checksum = hash_map[photo.checksum]
                num_correct_photos += 1
            else:
                photo.checksum = f"{photo.checksum}:{old_algo}"
                num_skipped_photos += 1
        self.hash_algorithm = self.db['hash_algorithm'] = new_algo
        print(f"Mapped {num_correct_photos} items")
        if num_skipped_photos:
            print(f"Skipped {num_skipped_photos} items")
        return num_skipped_photos

    def update_stored_filename_hashes(
            self,
            directory: Union[str, PathLike],
            verify: bool = True,
            dry_run: bool = False,
    ) -> dict:
        """Updates filenames to match checksums
        Run after mapping hashes to new algorithm.
        Skips files whose filename checksum matches the stored checksum

        :param directory: the photo storage directory
        :param verify: if True, verify that file checksums match
        :param dry_run: if True, perform a dry run and do not move photos
        :return: the number of missing or incorrect files not moved"""
        num_correct_photos = num_skipped_photos = num_incorrect_photos = num_missing_photos = 0
        directory = Path(directory).expanduser().resolve()
        stored_photos = [photo for photos in self.photo_db.values() for photo in photos if photo.store_path]
        total_file_size = sum(photo.file_size for photo in stored_photos)
        print(f"Updating {len(stored_photos)} filename hashes")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        logger = logging.getLogger()
        file_map = {}
        for photo in tqdm(stored_photos):
            abs_store_path = directory / photo.store_path
            new_store_path = f"{photo.store_path[:32]}{photo.checksum[:7]}{photo.store_path[39:]}"
            new_abs_store_path = directory / new_store_path
            if new_abs_store_path.exists():
                num_skipped_photos += 1
            elif not abs_store_path.exists():
                tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
            elif photo.store_path[32:39] == photo.checksum[:7]:
                num_skipped_photos += 1
            elif not verify or file_checksum(abs_store_path, self.hash_algorithm) == photo.checksum:
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(f"{'Will move' if dry_run else 'Moving'} {abs_store_path} to {new_abs_store_path}")
                file_map[str(abs_store_path)] = str(new_abs_store_path)
                if not dry_run:
                    os.rename(abs_store_path, new_abs_store_path)
                    photo.store_path = new_store_path
                num_correct_photos += 1
            else:
                tqdm.write(f"Incorrect checksum: {abs_store_path}")
                num_incorrect_photos += 1
        print(f"{'Would move' if dry_run else 'Moved'} {num_correct_photos} items")
        if num_skipped_photos:
            print(f"Skipped {num_skipped_photos} items")
        if num_incorrect_photos or num_missing_photos:
            print(f"Found {num_incorrect_photos} incorrect and {num_missing_photos} missing items")
        return file_map
