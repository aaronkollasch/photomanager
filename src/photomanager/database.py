from __future__ import annotations

from os import PathLike, rename, cpu_count, makedirs
from os.path import exists
from math import log
import random
from pathlib import Path
from datetime import datetime, tzinfo
import gzip
import logging
from typing import Union, Optional, Type, TypeVar
from collections.abc import Iterable, Container
import shlex

from tqdm import tqdm
import orjson
import zstandard as zstd
import xxhash
import blake3

from photomanager import PhotoManagerBaseException
from photomanager.hasher import (
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
        quotient = float(num) / 1024**exponent
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
        self._hash: int = hash(self)

    def __eq__(self, other: DB) -> bool:
        return self.db == other.db

    def __hash__(self) -> int:
        return hash(blake3.blake3(self.json, max_threads=blake3.blake3.AUTO).digest())

    def reset_saved(self):
        self._hash = hash(self)

    def is_modified(self) -> bool:
        return self._hash != hash(self)

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
    def sources(self) -> str:
        for photos in self.photo_db.values():
            for photo in photos:
                yield photo.src

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

        self.reset_saved()

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

    def to_file(self, path: Union[str, PathLike], overwrite: bool = False) -> None:
        """Save the Database to path.

        :param path: the destination path
        :param overwrite: if false, do not overwrite an existing database at `path`
            and instead rename the it based on its last modified timestamp.
        """
        logger = logging.getLogger(__name__)
        logger.debug(f"Saving database to {path}")
        path = Path(path)
        if not overwrite and path.is_file():
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

    def save(
        self,
        path: Union[str, PathLike],
        argv: list[str],
        overwrite: bool = False,
        force: bool = False,
        collect_db: bool = False,
        destination: Optional[Union[str, PathLike]] = None,
    ) -> bool:
        """Save the database if it has been modified.

        :param path: the destination path
        :param overwrite: if false, do not overwrite an existing database at `path`
            and instead rename the it based on its last modified timestamp.
        :param argv: Add argv to the databases command_history
        :param force: save even if not modified
        :param collect_db: also collect the database to the storage destination
        :param destination: the base storage directory for collect_db
        :return True if save was successful
        """
        if force or self.is_modified():
            self.add_command(shlex.join(["photomanager"] + argv[1:]))
            try:
                self.to_file(path, overwrite=overwrite)
            except (OSError, PermissionError):  # pragma: no cover
                return False
            if collect_db and destination:
                try:
                    makedirs(Path(destination) / "database", exist_ok=True)
                    self.to_file(Path(destination) / "database" / Path(path).name)
                except (OSError, PermissionError):  # pragma: no cover
                    return False
            self.reset_saved()
            return True
        else:
            logging.info("The database was not modified and will not be saved")
            return False

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
        """
        next_uid = "".join(random.choices(self.UID_ALPHABET, k=8))
        if next_uid in self.photo_db:  # pragma: no cover
            # 8 base58 characters ~= 10^14 possible uids.
            # P(collision) ≈ 50% at 1 million uids, so we must check for collisions.
            # However, collisions cannot be easily replicated in testing,
            # so this branch will not be checked.
            return self.generate_uuid()
        return next_uid

    def add_photo(self, photo: PhotoFile, uid: Optional[str]) -> Optional[str]:
        """
        Adds a photo into the database with specified uid (can be None).

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

    def add_photos(self, photos: Iterable[PhotoFile]) -> tuple[set[str], int, int, int]:
        """
        Add photos to the database.
        :param photos: an Iterable of photos to add
        :return: the uids changed, number of photos added, merged, and skipped
        """
        changed_uids = set()
        num_added_photos = num_merged_photos = num_skipped_photos = 0
        for pf in photos:
            uid = self.find_photo(photo=pf)
            result = self.add_photo(photo=pf, uid=uid)

            if result is None:  # photo not added
                num_skipped_photos += 1
            elif uid is None:  # new uid added
                num_added_photos += 1
                changed_uids.add(result)
            else:  # photo already in database
                num_merged_photos += 1
                changed_uids.add(result)
        return changed_uids, num_added_photos, num_merged_photos, num_skipped_photos

    def get_photos_to_collect(
        self,
        directory: Union[str, PathLike],
        filter_uids: Optional[Container[str]] = None,
    ) -> tuple[list[tuple[PhotoFile, Optional[str]]], tuple[int, int, int, int]]:
        """
        Finds photos that can be collected

        Returns only photos that have a store_path set or that have the highest
        priority and whose checksum does not match any stored alternative photo

        :param directory: the photo storage directory
        :param filter_uids: optional, only collect the specified photo uids
        :return: PhotoFiles to copy, and their destination path,
            and the numbers of copied, added, missed and stored photos
        """
        logger = logging.getLogger(__name__)
        directory = Path(directory).expanduser().resolve()
        num_copied_photos = num_added_photos = num_missed_photos = num_stored_photos = 0
        photos_to_copy: list[tuple[PhotoFile, Optional[str]]] = []
        logger.info("Checking stored photos")
        if filter_uids is not None:
            photo_db = {
                uid: photos
                for uid, photos in self.photo_db.items()
                if uid in filter_uids
            }
        else:
            photo_db = self.photo_db
        for uid, photos in tqdm(photo_db.items()):
            highest_priority = min(photo.prio for photo in photos)
            stored_checksums: dict[str, int] = {}
            photos_marked_as_stored = [photo for photo in photos if photo.sto]
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
            highest_priority_photos = [
                photo
                for photo in photos
                if photo.prio == highest_priority and not photo.sto
            ]
            for photo in highest_priority_photos:
                rel_store_path = (
                    f"{photo.local_datetime.strftime('%Y/%m-%b/%Y-%m-%d_%H-%M-%S')}-"
                    f"{photo.chk[:7]}-"
                    f"{Path(photo.src).name}"
                )
                abs_store_path = directory / rel_store_path
                if (
                    photo.chk in stored_checksums
                    and stored_checksums[photo.chk] <= photo.prio
                ):
                    logger.debug(f"Photo duplicate already stored: {photo.src}")
                    num_stored_photos += 1
                elif abs_store_path.exists():
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
        return (
            photos_to_copy,
            (num_copied_photos, num_added_photos, num_missed_photos, num_stored_photos),
        )

    def get_photos_to_remove(
        self,
        directory: Union[str, PathLike],
        subdirectory: Union[str, PathLike] = "",
        dry_run: bool = False,
    ) -> list[PhotoFile]:
        """
        Finds lower-priority stored photos to remove

        Returns lower-priority photos if a higher-priority alternative is stored.

        If a lower-priority version is marked as stored and its checksum matches
        a higher-priority stored photo, it will be "de-listed":
        it will no longer be marked as stored.

        :param directory: the photo storage directory
        :param subdirectory: remove only photos within subdirectory
        :param dry_run: if True, do not delist photos
        :return: the number of photos removed
        """
        logger = logging.getLogger(__name__)
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
                    else:
                        logger.debug(
                            f"{'Will de-list' if dry_run else 'De-listing'}: "
                            f"entry {photo.src} stored in {abs_store_path}"
                        )
                        if not dry_run:
                            photo.sto = ""
        return photos_to_remove

    def get_stored_photos(
        self,
        subdirectory: Union[str, PathLike] = "",
    ):
        subdirectory = Path(subdirectory)
        if subdirectory.is_absolute():
            raise DatabaseException("Absolute subdirectory not supported")
        stored_photos = []
        for photos in self.photo_db.values():
            for photo in photos:
                if photo.sto and path_is_relative_to(Path(photo.sto), subdirectory):
                    stored_photos.append(photo)
        return stored_photos

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
        return num_uids, num_photos, num_stored_photos, total_file_size
