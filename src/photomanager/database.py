from __future__ import annotations

from os import PathLike, rename, cpu_count, makedirs, chmod, remove
from os.path import exists
import stat
from math import log
import random
from pathlib import Path
from datetime import datetime, tzinfo
import shutil
import gzip
import logging
import traceback
from typing import Union, Optional, Type, TypeVar
from collections.abc import Collection

from tqdm import tqdm
import orjson
import zstandard as zstd
import xxhash

from photomanager import PhotoManagerBaseException
from photomanager.pyexiftool import ExifTool, AsyncExifTool
from photomanager.hasher import (
    AsyncFileHasher,
    file_checksum,
    DEFAULT_HASH_ALGO,
    HashAlgorithm,
)
from photomanager.photofile import PhotoFile, NAME_MAP_ENC


unit_list = list(zip(["bytes", "kB", "MB", "GB", "TB", "PB"], [0, 0, 1, 2, 2, 2]))


def sizeof_fmt(num: int) -> str:
    """Human friendly file size
    https://stackoverflow.com/questions/1094841/
    get-human-readable-version-of-file-size"""
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


def tz_str_to_tzinfo(tz: str):
    """
    Convert a timezone string (e.g. -0400) to a tzinfo
    If "local", return None
    """
    if tz == "local":
        return None
    try:
        return datetime.strptime(tz, "%z").tzinfo
    except ValueError:
        pass


class DatabaseException(PhotoManagerBaseException):
    pass


DB = TypeVar("DB", bound="Database")


class Database:
    VERSION = 3
    """
    Database version history:
    2: added tz_offset
    3: shortened PhotoFile attribute names
    """
    DB_KEY_ORDER = (
        "version",
        "hash_algorithm",
        "timezone_default",
        "photo_db",
        "command_history",
    )

    def __init__(self):
        self._db: dict = {
            "version": self.VERSION,
            "hash_algorithm": DEFAULT_HASH_ALGO,
            "timezone_default": "local",
            "photo_db": {},
            "command_history": {},
        }
        self.hash_to_uid: dict[str, str] = {}
        self.timestamp_to_uids: dict[float, dict[str, None]] = {}

    def __eq__(self, other: DB) -> bool:
        return self.db == other.db

    @property
    def version(self) -> int:
        """Get the Database version number."""
        return self.db["version"]

    @property
    def hash_algorithm(self) -> HashAlgorithm:
        """Get the Database hash algorithm."""
        return self.db["hash_algorithm"]

    @hash_algorithm.setter
    def hash_algorithm(self, new_algorithm: HashAlgorithm):
        """Set the Database hash algorithm."""
        self.db["hash_algorithm"] = new_algorithm

    @property
    def photo_db(self) -> dict[str, list[PhotoFile]]:
        """Get the Database photo db."""
        return self.db["photo_db"]

    @property
    def timezone_default(self) -> Optional[tzinfo]:
        """Get the Database default time zone.

        :return: the time zone as a datetime.tzinfo,
        """
        tz_default = self.db.get("timezone_default", "local")
        return tz_str_to_tzinfo(tz_default)

    @property
    def command_history(self) -> dict[str, str]:
        """Get the Database command history."""
        return self.db["command_history"]

    @property
    def db(self) -> dict:
        """Get the Database parameters as a dict."""
        return self._db

    @db.setter
    def db(self, db: dict):
        """Set the Database parameters from a dict."""
        db.setdefault("version", 1)  # legacy dbs are version 1
        db.setdefault("hash_algorithm", "sha256")  # legacy dbs use sha256
        db.setdefault("timezone_default", "local")  # legacy dbs are in local time

        db["version"] = int(db["version"])
        if db["version"] > self.VERSION:
            raise DatabaseException(
                "Database version too new for this version of PhotoManager."
            )
        if db["version"] < 3:
            for uid in db["photo_db"].keys():
                photos = db["photo_db"][uid]
                for i in range(len(photos)):
                    photos[i] = {NAME_MAP_ENC[k]: v for k, v in photos[i].items()}

        db = {k: db[k] for k in self.DB_KEY_ORDER}
        db["hash_algorithm"] = HashAlgorithm(db["hash_algorithm"])
        for uid in db["photo_db"].keys():
            db["photo_db"][uid] = [PhotoFile.from_dict(d) for d in db["photo_db"][uid]]

        db["version"] = self.VERSION
        self._db = db

        for uid, photos in self.photo_db.items():
            for photo in photos:
                self.hash_to_uid[photo.chk] = uid
                if photo.ts in self.timestamp_to_uids:
                    self.timestamp_to_uids[photo.ts][uid] = None
                else:
                    self.timestamp_to_uids[photo.ts] = {uid: None}

    @classmethod
    def from_dict(cls: Type[DB], db_dict: dict) -> DB:
        """Load a Database from a dictionary. Warning: can modify the dictionary."""
        db = cls()
        db.db = db_dict
        return db

    @property
    def json(self) -> bytes:
        """Get the Database parameters as json data."""
        return self.to_json(pretty=False)

    @json.setter
    def json(self, json_data: bytes):
        """Set the Database parameters from json data."""
        db = orjson.loads(json_data)
        self.db = db

    def to_json(self, pretty=False) -> bytes:
        """Get the Database parameters as json data.

        :param pretty: If True, pretty-print the json output.
        """
        return orjson.dumps(self.db, option=orjson.OPT_INDENT_2 if pretty else 0)

    @classmethod
    def from_json(cls: Type[DB], json_data: bytes) -> DB:
        """Load a Database from JSON data."""
        db = cls()
        db.json = json_data
        return db

    @classmethod
    def from_file(cls: Type[DB], path: Union[str, PathLike], create_new=False) -> DB:
        """Load a Database from a path."""
        path = Path(path)
        if not path.exists() and create_new:
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
                        f"zstd content checksum verification failed: "
                        f"{checksum.hex()} != {s_hash.hex()}"
                    )
        else:
            with open(path, "rb") as f:
                s = f.read()

        db = orjson.loads(s)
        del s
        db = cls.from_dict(db)
        return db

    def to_file(self, path: Union[str, PathLike]) -> None:
        """Save the Database to path.

        Moves an existing database at that path to a new location
        based on its last modified timestamp.
        """
        logger = logging.getLogger(__name__)
        logger.debug(f"Saving database to {path}")
        path = Path(path)
        if path.is_file():
            base_path = path
            for _ in path.suffixes:
                base_path = base_path.with_suffix("")
            timestamp_str = datetime.fromtimestamp(path.stat().st_mtime).strftime(
                "%Y-%m-%d_%H-%M-%S"
            )
            new_path = base_path.with_name(
                f"{base_path.name}_{timestamp_str}"
            ).with_suffix("".join(path.suffixes))
            logger.debug(f"Moving old database at {path} to {new_path}")
            try:
                rename(path, new_path)
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
                    new_paths = list(
                        base_path.parent.glob(
                            base_path.name + "_*" + "".join(path.suffixes)
                        )
                    )
                    max_version = 0
                    for p in new_paths:
                        bp = p
                        for _ in p.suffixes:
                            bp = bp.with_suffix("")
                        try:
                            version = int(bp.name.rsplit("_", 1)[1])
                            if version > max_version:
                                max_version = version
                        except ValueError:
                            pass
                    version = max_version
                    base_path = base_path.with_name(
                        base_path.name + "_" + str(version + 1)
                    )
                path = base_path.with_name(base_path.name + "".join(path.suffixes))
                logger.info(f"Saving new database to alternate path {path}")

        save_bytes = self.to_json(pretty=True)
        if path.suffix == ".gz":
            with gzip.open(path, "wb", compresslevel=5) as f:
                f.write(save_bytes)
        elif path.suffix == ".zst":
            with open(path, "wb") as f:
                cctx = zstd.ZstdCompressor(
                    level=7,
                    write_checksum=True,
                    threads=cpu_count(),
                )
                f.write(cctx.compress(save_bytes))
        else:
            with open(path, "wb") as f:
                f.write(save_bytes)

    def add_command(self, command: str) -> str:
        """Adds a command to the command history.

        Creates a timestamp string with the current date, time, and time zone.

        :return the timestamp string
        """
        dt = datetime.now().astimezone().strftime("%Y-%m-%d_%H-%M-%S%z")
        self.command_history[dt] = command
        return dt

    def find_photo(self, photo: PhotoFile) -> Optional[str]:
        """Finds a photo in the database and returns its uid.

        Matches first by file checksum, then by timestamp+filename (case-insensitive).

        :return the photo's uid, or None if not found
        """
        logger = logging.getLogger(__name__)
        if photo.chk in self.hash_to_uid:
            return self.hash_to_uid[photo.chk]
        uids = self.timestamp_to_uids.get(photo.ts, None)
        if uids:
            name_matches = []
            photo_name = Path(photo.src).name
            for uid in uids:
                if any(
                    photo_name.lower() == Path(pf.src).name.lower()
                    for pf in self.photo_db[uid]
                ):
                    name_matches.append(uid)
            if name_matches:
                if len(name_matches) > 1:
                    logger.warning(
                        f"ambiguous timestamp+name match: "
                        f"{photo.src}: {name_matches}"
                    )
                return name_matches[0]
        return None

    UID_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

    def generate_uuid(self) -> str:
        """
        Generate a new uid that is not in the photo_db.
        8 base58 characters = 10^14 possible uids.
        P(collision) ??? 50% at 1 million uids, so we must check for collisions.
        """
        next_uid = "".join(random.choices(self.UID_ALPHABET, k=8))
        if next_uid in self.photo_db:  # pragma: no cover
            return self.generate_uuid()
        return next_uid

    def add_photo(self, photo: PhotoFile, uid: Optional[str]) -> Optional[str]:
        """Adds a photo into the database with specified uid (can be None).

        Skips and returns None if the photo checksum is already in the database
        under a different uid or the photo checksum+source_path pair is already
        in the database.

        :param photo: the PhotoFile to add
        :param uid: the uid for the group of PhotoFiles to add to.
            If uid is None, a new random uid will be generated.
        :return the added photo's uid, or None if not added
        """
        if photo.chk in self.hash_to_uid:
            if uid is not None and uid != self.hash_to_uid[photo.chk]:
                return None
            if any(
                photo.chk == p.chk and photo.src == p.src
                for p in self.photo_db[self.hash_to_uid[photo.chk]]
            ):
                return None
        if uid is None:
            if photo.chk in self.hash_to_uid:
                uid: str = self.hash_to_uid[photo.chk]
            else:
                uid: str = self.generate_uuid()
        if uid in self.photo_db:
            photos = self.photo_db[uid]
            assert not any(photo.chk == p.chk and photo.src == p.src for p in photos)
            if non_matching_checksums := set(
                p.chk for p in photos if photo.src == p.src and photo.chk != p.chk
            ):
                logging.warning(
                    f"Checksum of previously-indexed source photo has changed: "
                    f"{repr(photo.chk)} not in {non_matching_checksums}"
                )
            photos.append(photo)
            photos.sort(key=lambda pf: pf.prio)
        else:
            self.photo_db[uid] = [photo]
        self.hash_to_uid[photo.chk] = uid
        if photo.ts in self.timestamp_to_uids:
            self.timestamp_to_uids[photo.ts][uid] = None
        else:
            self.timestamp_to_uids[photo.ts] = {uid: None}
        return uid

    def index_photos(
        self,
        files: Collection[Union[str, PathLike]],
        priority: int = 10,
        timezone_default: Optional[str] = None,
        storage_type: str = "HDD",
    ) -> (int, int, int, int):
        """Indexes photo files and adds them to the database with a designated priority.

        :param files: the photo file paths to index
        :param priority: the photos' priority
        :param timezone_default: the default timezone to use when importing
            If None, use the database default
        :param storage_type: the storage type being indexed (uses more async if SSD)
        :return: the number of photos added, merged, skipped, or errored
        """
        logger = logging.getLogger(__name__)
        num_added_photos = num_merged_photos = num_skipped_photos = num_error_photos = 0
        if storage_type in ("SSD", "RAID"):
            async_hashes = True
            async_exif = cpu_count()
        else:
            async_hashes = (
                False  # concurrent reads of sequential files can lead to thrashing
            )
            async_exif = min(
                4, cpu_count()
            )  # exiftool is partially CPU-bound and benefits from async
        logger.info("Collecting media hashes")
        checksum_cache = AsyncFileHasher(
            algorithm=self.hash_algorithm, use_async=async_hashes
        ).check_files(files, pbar_unit="B")
        logger.info("Collecting media dates and times")
        datetime_cache = AsyncExifTool(num_workers=async_exif).get_best_datetime_batch(
            files
        )
        timezone_default = (
            tz_str_to_tzinfo(timezone_default)
            if timezone_default is not None
            else self.timezone_default
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
                    tz_default=timezone_default,
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
            except Exception as e:  # pragma: no cover
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
                f"Skipped {num_skipped_photos} items and errored on "
                f"{num_error_photos} items"
            )
        return num_added_photos, num_merged_photos, num_skipped_photos, num_error_photos

    def collect_to_directory(
        self, directory: Union[str, PathLike], dry_run: bool = False
    ) -> (int, int, int, int):
        """Collects photos in the database into a directory.

        Collects only photos that have a store_path set or that have the highest
        priority and whose checksum does not match any stored alternative photo

        Updates the store_path for photos newly stored. Store_paths are
        relative to the storage directory.
        Stored photos have permissions set to read-only for all.

        :param directory the photo storage directory
        :param dry_run if True, do not copy photos
        :return the number of photos that could not be collected
        """
        logger = logging.getLogger(__name__)
        directory = Path(directory).expanduser().resolve()
        num_copied_photos = num_added_photos = num_missed_photos = num_stored_photos = 0
        photos_to_copy = []
        logger.info("Checking stored photos")
        for uid, photos in tqdm(self.photo_db.items()):
            highest_priority = min(photo.prio for photo in photos)
            stored_checksums = {}
            photos_marked_as_stored = [photo for photo in photos if photo.sto]
            highest_priority_photos = [
                photo
                for photo in photos
                if photo.prio == highest_priority and not photo.sto
            ]
            for photo in photos_marked_as_stored:
                abs_store_path = directory / photo.sto
                if abs_store_path.exists():
                    stored_checksums[photo.chk] = min(
                        stored_checksums.get(photo.chk, photo.prio),
                        photo.prio,
                    )
                    num_stored_photos += 1
                elif exists(photo.src):
                    photos_to_copy.append((photo, None))
                    stored_checksums[photo.chk] = min(
                        stored_checksums.get(photo.chk, photo.prio),
                        photo.prio,
                    )
                    num_copied_photos += 1
                else:
                    logger.warning(f"Photo not found: {photo.src}")
                    num_missed_photos += 1
            for photo in highest_priority_photos:
                assert not (
                    photo.chk in stored_checksums
                    and photo.prio >= stored_checksums[photo.chk]
                )
                rel_store_path = (
                    f"{photo.local_datetime.strftime('%Y/%m-%b/%Y-%m-%d_%H-%M-%S')}-"
                    f"{photo.chk[:7]}-"
                    f"{Path(photo.src).name}"
                )
                abs_store_path = directory / rel_store_path
                if abs_store_path.exists():
                    logger.debug(f"Photo already present: {abs_store_path}")
                    photo.sto = rel_store_path
                    stored_checksums[photo.chk] = min(
                        stored_checksums.get(photo.chk, photo.prio),
                        photo.prio,
                    )
                    num_stored_photos += 1
                elif exists(photo.src):
                    photos_to_copy.append((photo, rel_store_path))
                    stored_checksums[photo.chk] = min(
                        stored_checksums.get(photo.chk, photo.prio),
                        photo.prio,
                    )
                    num_added_photos += 1
                else:
                    logger.warning(f"Photo not found: {photo.src}")
                    num_missed_photos += 1

        estimated_copy_size = sum(photo.fsz for photo, _ in photos_to_copy)
        logger.info(
            f"{'Will copy' if dry_run else 'Copying'} {len(photos_to_copy)} items, "
            f"estimated size: {sizeof_fmt(estimated_copy_size)}"
        )
        p_bar = tqdm(
            total=estimated_copy_size, unit="B", unit_scale=True, unit_divisor=1024
        )
        for photo, rel_store_path in photos_to_copy:
            if rel_store_path is None:
                abs_store_path = directory / photo.sto
            else:
                abs_store_path = directory / rel_store_path
            if logger.isEnabledFor(logging.DEBUG):
                tqdm.write(
                    f"{'Will copy' if dry_run else 'Copying'}: {photo.src} "
                    f"to {abs_store_path}"
                )
            if not dry_run:
                makedirs(abs_store_path.parent, exist_ok=True)
                shutil.copy2(photo.src, abs_store_path)
                chmod(abs_store_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                if rel_store_path is not None:
                    photo.sto = rel_store_path
            p_bar.update(photo.fsz)
        p_bar.close()

        print(
            f"Copied {len(photos_to_copy)} items, estimated size: "
            f"{sizeof_fmt(estimated_copy_size)}: "
            f"{num_added_photos} new items and {num_copied_photos} "
            f"items marked as stored elsewhere"
        )
        if num_stored_photos or num_missed_photos:
            print(
                f"Skipped {num_stored_photos} items already stored "
                f"and {num_missed_photos} missing items"
            )
        return num_copied_photos, num_added_photos, num_missed_photos, num_stored_photos

    def clean_stored_photos(
        self,
        directory: Union[str, PathLike],
        subdirectory: Union[str, PathLike] = "",
        dry_run: bool = False,
    ) -> (int, int, float):
        """Removes lower-priority stored photos if a higher-priority version is stored.

        :param directory the photo storage directory
        :param subdirectory remove only photos within subdirectory
        :param dry_run if True, do not remove photos
        :return the number of photos removed
        """
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
                photo.prio
                for photo in photos
                if photo.sto and (directory / photo.sto).exists()
            )
            highest_priority_checksums = set(
                photo.chk
                for photo in photos
                if photo.prio == highest_stored_priority
                and (directory / photo.sto).exists()
            )
            for photo in photos:
                abs_store_path = directory / photo.sto
                if (
                    photo.prio > highest_stored_priority
                    and photo.sto
                    and path_is_relative_to(abs_store_path, abs_subdirectory)
                ):
                    if photo.chk not in highest_priority_checksums:
                        photos_to_remove.append(photo)
                        total_file_size += photo.fsz
                    else:
                        logger.debug(
                            f"{'Will de-list' if dry_run else 'De-listing'}: "
                            f"entry {photo.src} stored in {abs_store_path}"
                        )
                        if not dry_run:
                            photo.sto = ""
        print(f"Identified {len(photos_to_remove)} lower-priority items for removal")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        for photo in tqdm(photos_to_remove):
            abs_store_path = directory / photo.sto
            if abs_store_path.exists():
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(
                        f"{'Will remove' if dry_run else 'Removing'}: {abs_store_path}"
                    )
                if not dry_run:
                    remove(abs_store_path)
                    photo.sto = ""
                num_removed_photos += 1
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
        print(
            f"{'Found' if dry_run else 'Removed'} {num_removed_photos} items "
            f"and skipped {num_missing_photos} missing items"
        )
        return num_removed_photos, num_missing_photos, total_file_size

    def verify_stored_photos(
        self,
        directory: Union[str, PathLike],
        subdirectory: Union[str, PathLike] = "",
        storage_type: str = "HDD",
    ) -> int:
        """Check the files stored in directory against checksums in the database.

        :param directory: the photo storage directory
        :param subdirectory: verify only photos within subdirectory
        :param storage_type: the type of media the photos are stored on
            (uses async if SSD)
        :return: the number of errors found
        """
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
                abs_store_path = directory / photo.sto
                if photo.sto and path_is_relative_to(abs_store_path, abs_subdirectory):
                    stored_photos.append(photo)
                    total_file_size += photo.fsz
        print(f"Verifying {len(stored_photos)} items")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        if storage_type in ("SSD", "RAID"):
            files, sizes = [], []
            for photo in stored_photos:
                abs_store_path = directory / photo.sto
                if abs_store_path.exists():
                    files.append(str(abs_store_path))
                    sizes.append(photo.fsz)
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
            abs_store_path = directory / photo.sto
            if not abs_store_path.exists():
                tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
            elif (
                checksum_cache[str(abs_store_path)]
                if str(abs_store_path) in checksum_cache
                else file_checksum(abs_store_path, self.hash_algorithm)
            ) == photo.chk:
                num_correct_photos += 1
            else:
                tqdm.write(f"Incorrect checksum: {abs_store_path}")
                num_incorrect_photos += 1
            if checksum_cache:
                p_bar.update()
            else:
                p_bar.update(photo.fsz)
        p_bar.close()

        print(
            f"Checked "
            f"{num_correct_photos+num_incorrect_photos+num_missing_photos} "
            f"items"
        )
        if num_incorrect_photos or num_missing_photos:
            print(
                f"Found {num_incorrect_photos} incorrect and "
                f"{num_missing_photos} missing items"
            )
        else:
            print("No errors found")
        return num_incorrect_photos + num_missing_photos

    def verify_indexed_photos(self):
        """Check available source files against checksums in the database."""
        raise NotImplementedError

    def get_stats(self) -> tuple[int, int, int, int]:
        """Get database item statistics.

        :return num_uids, num_photos, num_stored_photos, total_file_size
        """
        num_uids = num_photos = num_stored_photos = total_file_size = 0
        for photos in self.photo_db.values():
            num_uids += 1
            for photo in photos:
                num_photos += 1
                if photo.sto:
                    num_stored_photos += 1
                    total_file_size += photo.fsz
        print(f"Total items:        {num_photos}")
        print(f"Total unique items: {num_uids}")
        print(f"Total stored items: {num_stored_photos}")
        print(f"Total file size:    {sizeof_fmt(total_file_size)}")
        return num_uids, num_photos, num_stored_photos, total_file_size

    def make_hash_map(
        self, new_algo: HashAlgorithm, hash_map: Optional[dict[str, str]] = None
    ) -> dict[str, str]:  # pragma: no cover
        """Make a map of file checksums in order to migrate hashing algorithms.

        Checks source file hashes using the old algorithm to make sure the new hashes
        are correct. If the source has an incorrect hash, does not map checksum and
        instead sets the mapped checksum to '{old_checksum}:{old_algorithm}'

        Note: This method is not accessed by the CLI or covered by testing
        and is intended to be used interactively, i.e. in a Jupyter notebook
        or other environment, with spot checking of the output hash map.

        :param new_algo: the new algorithm to use
        :param hash_map: a map from old hashes to new hashes; will be updated with
            new mappings as they are found
        :return: the hash map
        """
        if hash_map is None:
            hash_map = {}
        old_algo = self.hash_algorithm
        print(f"Converting {old_algo} to {new_algo}")
        num_correct_photos = (
            num_incorrect_photos
        ) = num_missing_photos = num_skipped_photos = 0
        all_photos = [photo for photos in self.photo_db.values() for photo in photos]
        for photo in tqdm(all_photos):
            if photo.chk in hash_map:
                num_skipped_photos += 1
            elif exists(photo.src):
                if photo.chk == file_checksum(photo.src, old_algo):
                    hash_map[photo.chk] = file_checksum(photo.src, new_algo)
                    num_correct_photos += 1
                else:
                    tqdm.write(f"Incorrect checksum: {photo.src}")
                    hash_map[photo.chk] = photo.chk + f":{old_algo}".encode()
                    num_incorrect_photos += 1
            else:
                num_missing_photos += 1

        print(f"Mapped {num_correct_photos} items")
        if num_skipped_photos:
            print(f"Skipped {num_skipped_photos} items")
        if num_incorrect_photos or num_missing_photos:
            print(
                f"Found {num_incorrect_photos} incorrect and "
                f"{num_missing_photos} missing items"
            )
        return hash_map

    def map_hashes(
        self, new_algo: str, hash_map: dict[str, str], map_all: bool = False
    ) -> Optional[int]:  # pragma: no cover
        """Map the database's checksums to a new algorithm.

        If a checksum cannot be mapped, it is appended by ':{old_algorithm}'
        to denote that it was made with a different algorithm

        Note: This method is not accessed by the CLI or covered by testing
        and is intended to be used interactively, i.e. in a Jupyter notebook
        or other environment, with spot checking of the newly mapped hashes.

        :param new_algo: the hashing algorithm used to make hash_map
        :param hash_map: the map from old hashes to new hashes
        :param map_all: set to True to enforce that all hashes must be in hash_map
            before mapping starts
        :return: the number of hashes not mapped
        """
        num_correct_photos = num_skipped_photos = 0
        old_algo = self.hash_algorithm
        all_photos = [photo for photos in self.photo_db.values() for photo in photos]
        if map_all and (
            num_skipped_photos := sum(
                photo.chk.split(":", 1)[0] not in hash_map for photo in all_photos
            )
        ):
            print(f"Not all items will be mapped: {num_skipped_photos}")
            return None
        for photo in tqdm(all_photos):
            if photo.chk in hash_map:
                photo.chk = hash_map[photo.chk]
                num_correct_photos += 1
            elif (ca := photo.chk.split(":", 1)) and len(ca) == 2:
                if c := hash_map.get(ca[0], None):
                    photo.chk = c
                num_correct_photos += 1
            else:
                photo.chk = f"{photo.chk}:{old_algo}"
                num_skipped_photos += 1
        self.hash_algorithm = new_algo
        print(f"Mapped {num_correct_photos} items")
        if num_skipped_photos:
            print(f"Did not map {num_skipped_photos} items")
        return num_skipped_photos

    def update_stored_filename_hashes(
        self,
        directory: Union[str, PathLike],
        verify: bool = True,
        dry_run: bool = False,
    ) -> dict[str, str]:  # pragma: no cover
        """Updates filenames to match checksums.

        Run after mapping hashes to new algorithm with self.map_hashes()
        Skips files whose filename checksum matches the stored checksum.

        Note: This method is not accessed by the CLI or covered by testing
        and is intended to be used interactively, i.e. in a Jupyter notebook
        or other environment, with dry runs and spot checking of proposed
        changes before they are performed. Use at your own risk.

        :param directory: the photo storage directory
        :param verify: if True, verify that file checksums match
        :param dry_run: if True, perform a dry run and do not move photos
        :return: the mapping of files moved
        """
        num_correct_photos = (
            num_skipped_photos
        ) = num_incorrect_photos = num_missing_photos = 0
        directory = Path(directory).expanduser().resolve()
        stored_photos = [
            photo for photos in self.photo_db.values() for photo in photos if photo.sto
        ]
        total_file_size = sum(photo.fsz for photo in stored_photos)
        print(f"Updating {len(stored_photos)} filename hashes")
        print(f"Total file size: {sizeof_fmt(total_file_size)}")
        logger = logging.getLogger()
        file_map = {}
        for photo in tqdm(stored_photos):
            abs_store_path = directory / photo.sto
            new_store_path = f"{photo.sto[:32]}{photo.chk[:7]}{photo.sto[39:]}"
            new_abs_store_path = directory / new_store_path
            if new_abs_store_path.exists():
                num_skipped_photos += 1
            elif not abs_store_path.exists():
                tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
            elif photo.sto[32:39] == photo.chk[:7]:
                num_skipped_photos += 1
            elif (
                not verify
                or file_checksum(abs_store_path, self.hash_algorithm) == photo.chk
            ):
                if logger.isEnabledFor(logging.DEBUG):
                    tqdm.write(
                        f"{'Will move' if dry_run else 'Moving'} {abs_store_path} "
                        f"to {new_abs_store_path}"
                    )
                file_map[str(abs_store_path)] = str(new_abs_store_path)
                if not dry_run:
                    rename(abs_store_path, new_abs_store_path)
                    photo.sto = new_store_path
                num_correct_photos += 1
            else:
                tqdm.write(f"Incorrect checksum: {abs_store_path}")
                num_incorrect_photos += 1
        print(f"{'Would move' if dry_run else 'Moved'} {num_correct_photos} items")
        if num_skipped_photos:
            print(f"Skipped {num_skipped_photos} items")
        if num_incorrect_photos or num_missing_photos:
            print(
                f"Found {num_incorrect_photos} incorrect and "
                f"{num_missing_photos} missing items"
            )
        return file_map
