from __future__ import annotations
import os
from os import PathLike
import stat
from math import log
from uuid import uuid4
from pathlib import Path
from datetime import datetime
import shutil
from dataclasses import dataclass, asdict
import gzip
import logging
import traceback
from typing import Union, Optional, Type, TypeVar
from collections.abc import Collection
from tqdm import tqdm
import orjson
import zstandard as zstd
import xxhash
from photomanager.pyexiftool import ExifTool
from photomanager.pyexiftool_async import AsyncExifTool
from photomanager.hasher_async import AsyncFileHasher, file_checksum, DEFAULT_HASH_ALGO

PF = TypeVar("PF", bound="PhotoFile")


@dataclass
class PhotoFile:
    """A dataclass describing a photo or other media file

    Attributes:
        :checksum (str): checksum of photo file
        :source_path (str): Absolute path where photo was found
        :datetime (str): Datetime string for best estimated creation date
        :timestamp (float): POSIX timestamp of best estimated creation date
        :file_size (int): Photo file size, in bytes
        :store_path (str): Relative path where photo is stored, empty if not stored
        :priority (int): Photo priority (lower is preferred)
    """

    checksum: str
    source_path: str
    datetime: str
    timestamp: float
    file_size: int
    store_path: str = ""
    priority: int = 10

    @classmethod
    def from_file(
        cls: Type[PF],
        source_path: Union[str, PathLike],
        algorithm: str = DEFAULT_HASH_ALGO,
        priority: int = 10,
    ) -> PF:
        """Create a PhotoFile for a given file

        :param source_path: The path to the file
        :param algorithm: The hashing algorithm to use
        :param priority: The photo's priority
        """
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
            store_path="",
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
        """Create a PhotoFile for a given file

        If source_path is in the checksum and datetime caches, uses the cached value
        instead of reading from the file.

        :param source_path: The path to the file
        :param checksum_cache: A mapping of source paths to known checksums
        :param datetime_cache: A mapping of source paths to datetime strings
        :param algorithm: The hashing algorithm to use for new checksums
        :param priority: The photo's priority
        """
        photo_hash: str = (
            checksum_cache[source_path]
            if source_path in checksum_cache
            else file_checksum(source_path, algorithm)
        )
        dt = (
            datetime_cache[source_path]
            if source_path in datetime_cache
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
            store_path="",
            priority=priority,
        )

    @classmethod
    def from_dict(cls: Type[PF], d: dict) -> PF:
        return cls(**d)

    def to_dict(self) -> dict:
        return asdict(self)


def datetime_str_to_object(ts_str: str) -> datetime:
    """Parses a datetime string into a datetime object"""
    if "." in ts_str:
        for fmt in ("%Y:%m:%d %H:%M:%S.%f%z", "%Y:%m:%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                pass
    else:
        for fmt in (
            "%Y:%m:%d %H:%M:%S%z",
            "%Y:%m:%d %H:%M:%S",
            "%Y:%m:%d %H:%M%z",
            "%Y:%m:%d %H:%M",
        ):
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                pass
    raise ValueError(f"Could not parse datetime str: {repr(ts_str)}")


def get_media_datetime(path: Union[str, PathLike]) -> str:
    """Gets the best known datetime string for a file"""
    return ExifTool().get_best_datetime(path)


unit_list = list(zip(["bytes", "kB", "MB", "GB", "TB", "PB"], [0, 0, 1, 2, 2, 2]))


def sizeof_fmt(num: int) -> str:
    """Human friendly file size
    https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size"""
    if num > 1:
        exponent = min(int(log(num, 1024)), len(unit_list) - 1)
        quotient = float(num) / 1024 ** exponent
        unit, num_decimals = unit_list[exponent]
        format_string = "{:.%sf} {}" % num_decimals
        return format_string.format(quotient, unit)
    if num == 0:
        return "0 bytes"
    if num == 1:
        return "1 byte"


def path_is_relative_to(
    subpath: Union[str, PathLike], path: Union[str, PathLike]
) -> bool:
    """Check if subpath is a child of path"""
    subpath, path = Path(subpath), Path(path)
    if hasattr(subpath, "is_relative_to"):  # added in Python 3.9
        return subpath.is_relative_to(path)
    else:
        return path in subpath.parents


class PhotoManagerBaseException(Exception):
    pass


class PhotoManagerException(PhotoManagerBaseException):
    pass


class DatabaseException(PhotoManagerBaseException):
    pass


DB = TypeVar("DB", bound="Database")


class Database:
    VERSION = 1
    DB_KEY_ORDER = ("version", "hash_algorithm", "photo_db", "command_history")

    def __init__(self):
        self.db: dict = {
            "version": self.VERSION,
            "hash_algorithm": DEFAULT_HASH_ALGO,
            "photo_db": {},
            "command_history": {},
        }
        self.hash_to_uid: dict[str, str] = {}
        self.timestamp_to_uids: dict[float, dict[str, None]] = {}

    @property
    def version(self) -> int:
        return self.db["version"]

    @property
    def hash_algorithm(self) -> str:
        return self.db["hash_algorithm"]

    @hash_algorithm.setter
    def hash_algorithm(self, new_algorithm: str):
        self.db["hash_algorithm"] = new_algorithm

    @property
    def photo_db(self) -> dict[str, list[PhotoFile]]:
        return self.db["photo_db"]

    @property
    def command_history(self) -> dict[str, str]:
        return self.db["command_history"]

    @property
    def json(self) -> bytes:
        return orjson.dumps(self.db, option=orjson.OPT_INDENT_2)

    @json.setter
    def json(self, json_data: bytes):
        """Sets Database parameters from json data"""
        db = orjson.loads(json_data)
        db.setdefault("version", "1")  # legacy dbs are version 1
        db.setdefault("hash_algorithm", "sha256")  # legacy dbs use sha256
        db = {k: db[k] for k in self.DB_KEY_ORDER}
        for uid in db["photo_db"].keys():
            db["photo_db"][uid] = [PhotoFile.from_dict(d) for d in db["photo_db"][uid]]
        self.db = db

        for uid, photos in self.photo_db.items():
            for photo in photos:
                self.hash_to_uid[photo.checksum] = uid
                if photo.timestamp in self.timestamp_to_uids:
                    self.timestamp_to_uids[photo.timestamp][uid] = None
                else:
                    self.timestamp_to_uids[photo.timestamp] = {uid: None}

    @classmethod
    def from_json(cls: Type[DB], json_data: bytes) -> DB:
        """Loads a Database from JSON data"""
        db = cls()
        db.json = json_data
        return db

    @classmethod
    def from_file(cls: Type[DB], path: Union[str, PathLike]) -> DB:
        """Loads a Database from a path"""
        path = Path(path)
        if not path.exists():
            logger = logging.getLogger(__name__)
            logger.warning(
                "Database file does not exist. Starting with blank database."
            )
            return cls()

        if path.suffix == ".gz":
            with gzip.open(path, "rb") as f:
                s = f.read()
        elif path.suffix == ".zst":
            with open(path, "rb") as f:
                c = f.read()
                has_checksum, checksum = (
                    zstd.get_frame_parameters(c).has_checksum,
                    c[-4:],
                )
                s = zstd.decompress(c)
                del c
                s_hash = xxhash.xxh64_digest(s)
                if has_checksum and checksum != s_hash[-4:][::-1]:
                    raise DatabaseException(
                        f"zstd content checksum verification failed: {checksum.hex()} != {s_hash.hex()}"
                    )
        else:
            with open(path, "rb") as f:
                s = f.read()

        db = cls.from_json(s)
        del s
        return db

    def to_file(self, path: Union[str, PathLike]) -> None:
        """Saves the db to path and moves an existing database at that path to a different location"""
        logger = logging.getLogger(__name__)
        logger.debug(f"Saving database to {path}")
        path = Path(path)
        if path.is_file():
            base_path = path
            for _ in path.suffixes:
                base_path = base_path.with_suffix("")
            new_path = base_path.with_name(
                f"{base_path.name}_"
                f"{datetime.fromtimestamp(path.stat().st_mtime).strftime('%Y-%m-%d_%H-%M-%S')}"
            ).with_suffix("".join(path.suffixes))
            if not new_path.exists():
                logger.debug(f"Moving old database at {path} to {new_path}")
                try:
                    os.rename(path, new_path)
                except OSError as e:
                    logger.warning(
                        f"Could not move old database from {path} to {new_path}. "
                        f"{type(e).__name__} {e}"
                    )
                    try:
                        name, version = base_path.name.rsplit("_", 1)
                        version = int(version)
                        base_path = base_path.with_name(name + "_" + str(version + 1))
                    except ValueError:
                        base_path = base_path.with_name(base_path.name + "_1")
                    path = base_path.with_name(base_path.name + "".join(path.suffixes))
                    logger.info(f"Saving new database to alternate path {path}")

        save_bytes = self.json
        if path.suffix == ".gz":
            with gzip.open(path, "wb", compresslevel=5) as f:
                f.write(save_bytes)
        elif path.suffix == ".zst":
            with open(path, "wb") as f:
                cctx = zstd.ZstdCompressor(level=5, write_checksum=True)
                f.write(cctx.compress(save_bytes))
        else:
            with open(path, "wb") as f:
                f.write(save_bytes)

    def add_command(self, command: str) -> str:
        """Adds a command to the command history

        Creates a timestamp string with the current date, time, and time zone.

        :return the timestamp string"""

        dt = datetime.now().astimezone().strftime("%Y-%m-%d_%H-%M-%S%z")
        self.command_history[dt] = command
        return dt

    def find_photo(self, photo: PhotoFile) -> Optional[str]:
        """Finds a photo in the database and returns its uid

        Matches first by file checksum, then by timestamp+filename (case-insensitive).

        :return the photo's uid, or None if not found"""
        logger = logging.getLogger(__name__)
        if photo.checksum in self.hash_to_uid:
            return self.hash_to_uid[photo.checksum]
        uids = self.timestamp_to_uids.get(photo.timestamp, None)
        if uids:
            name_matches = []
            photo_name = Path(photo.source_path).name
            for uid in uids:
                if any(
                    photo_name.lower() == Path(pf.source_path).name.lower()
                    for pf in self.photo_db[uid]
                ):
                    name_matches.append(uid)
            if name_matches:
                if len(name_matches) > 1:
                    logger.warning(
                        f"ambiguous timestamp+name match: {photo.source_path}: {name_matches}"
                    )
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
            if any(
                photo.checksum == pf.checksum and photo.source_path == pf.source_path
                for pf in self.photo_db[self.hash_to_uid[photo.checksum]]
            ):
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

    def index_photos(
        self,
        files: Collection[Union[str, PathLike]],
        priority: int = 10,
        storage_type: str = "HDD",
    ) -> int:
        """Indexes photo files and adds them to the database with a designated priority

        :param files: the photo file paths to index
        :param priority: the photos' priority
        :param storage_type: the storage type being indexed (uses more async if SSD)
        :return: the number of photos indexed"""
        logger = logging.getLogger(__name__)
        num_added_photos = num_merged_photos = num_skipped_photos = num_error_photos = 0
        if storage_type in ("SSD", "RAID"):
            async_hashes = True
            async_exif = os.cpu_count()
        else:
            async_hashes = (
                False  # concurrent reads of sequential files can lead to thrashing
            )
            async_exif = min(
                4, os.cpu_count()
            )  # exiftool is partially CPU-bound and benefits from async
        logger.info("Collecting media hashes")
        checksum_cache = AsyncFileHasher(
            algorithm=self.hash_algorithm, use_async=async_hashes
        ).check_files(files, pbar_unit="B")
        logger.info("Collecting media dates and times")
        datetime_cache = AsyncExifTool(num_workers=async_exif).get_best_datetime_batch(
            files
        )
        logger.info("Indexing media")
        exiftool = ExifTool()
        exiftool.start()
        for current_file in tqdm(files):
            if logger.isEnabledFor(logging.DEBUG):
                tqdm.write(f"Indexing {current_file}")
            try:
                pf = PhotoFile.from_file_cached(
                    current_file,
                    checksum_cache=checksum_cache,
                    datetime_cache=datetime_cache,
                    algorithm=self.hash_algorithm,
                    priority=priority,
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
                tqdm.write(f"Error indexing {current_file}")
                tb_str = traceback.format_exception(
                    etype=type(e), value=e, tb=e.__traceback__
                )
                tqdm.write(tb_str)
                num_error_photos += 1
        exiftool.terminate()

        print(f"Indexed {num_added_photos+num_merged_photos}/{len(files)} items")
        print(
            f"Added {num_added_photos} new items and merged {num_merged_photos} items"
        )
        if num_skipped_photos or num_error_photos:
            print(
                f"Skipped {num_skipped_photos} items and errored on {num_error_photos} items"
            )
        return num_added_photos + num_merged_photos

    def get_chosen_photos(self) -> list[PhotoFile]:
        """Gets all photos this database has stored or would choose to store"""
        chosen_photos = []
        for uid, photos in self.photo_db.items():
            highest_priority = min(photo.priority for photo in photos)
            new_chosen_photos = list(photo for photo in photos if photo.store_path)
            stored_checksums = set(photo.checksum for photo in new_chosen_photos)
            for photo in photos:
                if (
                    photo.priority == highest_priority
                    and photo.checksum not in stored_checksums
                ):
                    new_chosen_photos.append(photo)
                    stored_checksums.add(photo.checksum)
            chosen_photos.extend(new_chosen_photos)
        return chosen_photos

    def collect_to_directory(
        self, directory: Union[str, PathLike], dry_run: bool = False
    ) -> int:
        """Collects photos in the database into a directory

        Collects only photos that have a store_path set or that have the highest priority
        and whose checksum does not match any stored alternative photo

        Updates the store_path for photos newly stored. Store_paths are relative to the storage directory.
        Stored photos have permissions set to read-only for all.

        :param directory the photo storage directory
        :param dry_run if True, do not copy photos
        :return the number of photos collected"""
        logger = logging.getLogger(__name__)
        directory = Path(directory).expanduser().resolve()
        num_transferred_photos = (
            num_added_photos
        ) = num_missed_photos = num_stored_photos = 0
        photos_to_copy = []
        logger.info("Checking stored photos")
        for uid, photos in tqdm(self.photo_db.items()):
            highest_priority = min(photo.priority for photo in photos)
            stored_checksums = set()
            photos_marked_as_stored = [photo for photo in photos if photo.store_path]
            highest_priority_photos = [
                photo
                for photo in photos
                if photo.priority == highest_priority and not photo.store_path
            ]
            for photo in photos_marked_as_stored:
                abs_store_path = directory / photo.store_path
                if abs_store_path.exists():
                    stored_checksums.add(photo.checksum)
                    num_stored_photos += 1
                elif os.path.exists(photo.source_path):
                    photos_to_copy.append((photo, None))
                    stored_checksums.add(photo.checksum)
                    num_transferred_photos += 1
                else:
                    logger.debug(f"Photo not found: {photo.source_path}")
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
                    logger.debug(f"Photo already present: {abs_store_path}")
                    photo.store_path = rel_store_path
                    stored_checksums.add(photo.checksum)
                    num_stored_photos += 1
                elif os.path.exists(photo.source_path):
                    photos_to_copy.append((photo, rel_store_path))
                    stored_checksums.add(photo.checksum)
                    num_added_photos += 1
                else:
                    logger.debug(f"Photo not found: {photo.source_path}")
                    num_missed_photos += 1

        estimated_copy_size = sum(photo.file_size for photo, _ in photos_to_copy)
        logger.info(
            f"{'Will copy' if dry_run else 'Copying'} {len(photos_to_copy)} items, "
            f"estimated size: {sizeof_fmt(estimated_copy_size)}"
        )
        p_bar = tqdm(
            total=estimated_copy_size, unit="B", unit_scale=True, unit_divisor=1024
        )
        for photo, rel_store_path in photos_to_copy:
            if rel_store_path is None:
                abs_store_path = directory / photo.store_path
            else:
                abs_store_path = directory / rel_store_path
            if logger.isEnabledFor(logging.DEBUG):
                tqdm.write(
                    f"{'Will copy' if dry_run else 'Copying'}: {photo.source_path} to {abs_store_path}"
                )
            if not dry_run:
                os.makedirs(abs_store_path.parent, exist_ok=True)
                shutil.copy2(photo.source_path, abs_store_path)
                os.chmod(abs_store_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                if rel_store_path is not None:
                    photo.store_path = rel_store_path
            p_bar.update(photo.file_size)
        p_bar.close()

        print(
            f"Copied {len(photos_to_copy)} items, estimated size: {sizeof_fmt(estimated_copy_size)}: "
            f"{num_added_photos} new items and {num_transferred_photos} items marked as stored elsewhere"
        )
        if num_stored_photos or num_missed_photos:
            print(
                f"Skipped {num_stored_photos} items already stored and {num_missed_photos} missing items"
            )
        return num_added_photos + num_transferred_photos

    def clean_stored_photos(
        self,
        directory: Union[str, PathLike],
        subdirectory: Union[str, PathLike] = "",
        dry_run: bool = False,
    ) -> int:
        """Removes lower-priority stored photos if a higher-priority version is stored

        :param directory the photo storage directory
        :param subdirectory remove only photos within subdirectory
        :param dry_run if True, do not remove photos
        :return the number of photos removed"""
        logger = logging.getLogger(__name__)
        num_removed_photos = num_missing_photos = total_file_size = 0
        directory = Path(directory).expanduser().resolve()
        subdirectory = Path(subdirectory)
        if subdirectory.is_absolute():
            raise DatabaseException("Absolute subdirectory not supported")
        abs_subdirectory = directory / subdirectory
        photos_to_remove = []
        for photos in self.photo_db.values():
            highest_stored_priority = min(
                photo.priority
                for photo in photos
                if photo.store_path and (directory / photo.store_path).exists()
            )
            for photo in photos:
                abs_store_path = directory / photo.store_path
                if (
                    photo.priority > highest_stored_priority
                    and photo.store_path
                    and path_is_relative_to(abs_store_path, abs_subdirectory)
                ):
                    photos_to_remove.append(photo)
                    total_file_size += photo.file_size
        print(f"Identified {len(photos_to_remove)} lower-priority items for removal")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        for photo in tqdm(photos_to_remove):
            abs_store_path = directory / photo.store_path
            if abs_store_path.exists():
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(
                        f"{'Will remove' if dry_run else 'Removing'}: {abs_store_path}"
                    )
                if not dry_run:
                    os.remove(abs_store_path)
                    photo.store_path = ""
                num_removed_photos += 1
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
        print(
            f"{'Found' if dry_run else 'Removed'} {num_removed_photos} items "
            f"and skipped {num_missing_photos} missing items"
        )
        return num_removed_photos

    def verify_stored_photos(
        self,
        directory: Union[str, PathLike],
        subdirectory: Union[str, PathLike] = "",
        storage_type: str = "HDD",
    ) -> int:
        """Check the files stored in directory against checksums in the database

        :param directory: the photo storage directory
        :param subdirectory: verify only photos within subdirectory
        :param storage_type: the type of media the photos are stored on (uses async if SSD)
        :return: the number of errors found"""
        num_correct_photos = (
            num_incorrect_photos
        ) = num_missing_photos = total_file_size = 0
        directory = Path(directory).expanduser().resolve()
        subdirectory = Path(subdirectory)
        if subdirectory.is_absolute():
            raise DatabaseException("Absolute subdirectory not supported")
        abs_subdirectory = directory / subdirectory
        stored_photos = []
        for photos in self.photo_db.values():
            for photo in photos:
                abs_store_path = directory / photo.store_path
                if photo.store_path and path_is_relative_to(
                    abs_store_path, abs_subdirectory
                ):
                    stored_photos.append(photo)
                    total_file_size += photo.file_size
        print(f"Verifying {len(stored_photos)} items")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        if storage_type in ("SSD", "RAID"):
            files, sizes = [], []
            for photo in stored_photos:
                abs_store_path = directory / photo.store_path
                if abs_store_path.exists():
                    files.append(str(abs_store_path))
                    sizes.append(photo.file_size)
            print("Collecting media hashes")
            checksum_cache = AsyncFileHasher(algorithm=self.hash_algorithm).check_files(
                file_paths=files, pbar_unit="B", file_sizes=sizes
            )
            p_bar = tqdm(total=len(stored_photos))
        else:
            checksum_cache = {}
            p_bar = tqdm(
                total=total_file_size, unit="B", unit_scale=True, unit_divisor=1024
            )

        for photo in stored_photos:
            abs_store_path = directory / photo.store_path
            if not abs_store_path.exists():
                tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
            elif (
                checksum_cache[str(abs_store_path)]
                if str(abs_store_path) in checksum_cache
                else file_checksum(abs_store_path, self.hash_algorithm)
            ) == photo.checksum:
                num_correct_photos += 1
            else:
                tqdm.write(f"Incorrect checksum: {abs_store_path}")
                num_incorrect_photos += 1
            if checksum_cache:
                p_bar.update()
            else:
                p_bar.update(photo.file_size)
        p_bar.close()

        print(
            f"Checked {num_correct_photos+num_incorrect_photos+num_missing_photos} items"
        )
        if num_incorrect_photos or num_missing_photos:
            print(
                f"Found {num_incorrect_photos} incorrect and {num_missing_photos} missing items"
            )
        else:
            print("No errors found")
        return num_incorrect_photos + num_missing_photos

    def verify_indexed_photos(self):
        raise NotImplementedError()

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

    def make_hash_map(
        self, new_algo: str, hash_map: Optional[dict[str, str]] = None
    ) -> dict[str, str]:
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
        num_correct_photos = (
            num_incorrect_photos
        ) = num_missing_photos = num_skipped_photos = 0
        all_photos = [photo for photos in self.photo_db.values() for photo in photos]
        for photo in tqdm(all_photos):
            if photo.checksum in hash_map:
                num_skipped_photos += 1
            elif os.path.exists(photo.source_path):
                if photo.checksum == file_checksum(photo.source_path, old_algo):
                    hash_map[photo.checksum] = file_checksum(
                        photo.source_path, new_algo
                    )
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
            print(
                f"Found {num_incorrect_photos} incorrect and {num_missing_photos} missing items"
            )
        return hash_map

    def map_hashes(
        self, new_algo: str, hash_map: dict[str, str], map_all: bool = False
    ) -> Optional[int]:
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
        if map_all and (
            num_skipped_photos := sum(
                photo.checksum not in hash_map for photo in all_photos
            )
        ):
            print(f"Not all items will be mapped: {num_skipped_photos}")
            return None
        for photo in tqdm(all_photos):
            if photo.checksum in hash_map:
                photo.checksum = hash_map[photo.checksum]
                num_correct_photos += 1
            else:
                photo.checksum = f"{photo.checksum}:{old_algo}"
                num_skipped_photos += 1
        self.hash_algorithm = new_algo
        print(f"Mapped {num_correct_photos} items")
        if num_skipped_photos:
            print(f"Skipped {num_skipped_photos} items")
        return num_skipped_photos

    def update_stored_filename_hashes(
        self,
        directory: Union[str, PathLike],
        verify: bool = True,
        dry_run: bool = False,
    ) -> dict[str, str]:
        """Updates filenames to match checksums
        Run after mapping hashes to new algorithm.
        Skips files whose filename checksum matches the stored checksum

        :param directory: the photo storage directory
        :param verify: if True, verify that file checksums match
        :param dry_run: if True, perform a dry run and do not move photos
        :return: the number of missing or incorrect files not moved"""
        num_correct_photos = (
            num_skipped_photos
        ) = num_incorrect_photos = num_missing_photos = 0
        directory = Path(directory).expanduser().resolve()
        stored_photos = [
            photo
            for photos in self.photo_db.values()
            for photo in photos
            if photo.store_path
        ]
        total_file_size = sum(photo.file_size for photo in stored_photos)
        print(f"Updating {len(stored_photos)} filename hashes")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        logger = logging.getLogger()
        file_map = {}
        for photo in tqdm(stored_photos):
            abs_store_path = directory / photo.store_path
            new_store_path = (
                f"{photo.store_path[:32]}{photo.checksum[:7]}{photo.store_path[39:]}"
            )
            new_abs_store_path = directory / new_store_path
            if new_abs_store_path.exists():
                num_skipped_photos += 1
            elif not abs_store_path.exists():
                tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
            elif photo.store_path[32:39] == photo.checksum[:7]:
                num_skipped_photos += 1
            elif (
                not verify
                or file_checksum(abs_store_path, self.hash_algorithm) == photo.checksum
            ):
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(
                        f"{'Will move' if dry_run else 'Moving'} {abs_store_path} to {new_abs_store_path}"
                    )
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
            print(
                f"Found {num_incorrect_photos} incorrect and {num_missing_photos} missing items"
            )
        return file_map
