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
from typing import Union, Optional, Type, TypeVar
from collections.abc import Collection
from pyexiftool import ExifTool
from tqdm import tqdm

BLOCK_SIZE = 65536
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
    def from_file(cls: Type[PF], source_path: Union[str, PathLike], priority: int = 10) -> PF:
        photo_hash: str = file_checksum(source_path)
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


def file_checksum(path: Union[str, PathLike]) -> str:
    sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        while block := f.read(BLOCK_SIZE):
            sha256.update(block)
    return sha256.hexdigest()


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


def datetime_is_valid(timestamp: str) -> bool:
    if timestamp and isinstance(timestamp, str) and not timestamp.startswith('0000'):
        return True
    return False


def get_media_datetime(path: Union[str, PathLike]) -> str:
    """Gets the best known datetime string for a file"""
    exiftool = ExifTool()
    metadata = exiftool.get_metadata(path)
    timestamp = metadata.get('Composite:SubSecDateTimeOriginal', '')
    if timestamp and datetime_is_valid(timestamp):
        return timestamp
    timestamp = metadata.get('QuickTime:CreationDate', '')
    if timestamp and datetime_is_valid(timestamp):
        return timestamp
    if 'EXIF:DateTimeOriginal' in metadata and datetime_is_valid(metadata['EXIF:DateTimeOriginal']):
        subsec = metadata.get('EXIF:SubSecTimeOriginal', '')
        offset = metadata.get('EXIF:OffsetTimeOriginal', '')
        return f"{metadata['EXIF:DateTimeOriginal']}{'.' if subsec else ''}{subsec}{offset}"
    for tag in metadata.keys():
        if 'DateTimeOriginal' in tag and datetime_is_valid(metadata[tag]):
            return metadata[tag]
    for tag in metadata.keys():
        if 'CreateDate' in tag and datetime_is_valid(metadata[tag]):
            return metadata[tag]
    return metadata['File:FileModifyDate']


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


DB = TypeVar('DB', bound='Database')


class Database:
    def __init__(self):
        self.db: dict = {'photo_db': {}, 'command_history': {}}
        self.photo_db: dict[str, list[PhotoFile]] = self.db['photo_db']
        self.hash_to_uid: dict[str, str] = {}
        self.timestamp_to_uids: dict[float, dict[str, None]] = {}

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

    def add_photo(self, photo: PhotoFile, uid: str) -> Optional[str]:
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
            uid = uuid4().hex
        if uid in self.photo_db:
            self.photo_db[uid].append(photo)
            self.photo_db[uid].sort(key=lambda pf: pf.priority)
        else:
            self.photo_db[uid] = [photo]
        self.hash_to_uid[photo.checksum] = uid
        if photo.timestamp in self.timestamp_to_uids:
            self.timestamp_to_uids[photo.timestamp][uid] = None
        else:
            self.timestamp_to_uids[photo.timestamp] = {uid: None}
        return uid

    def import_photos(self, files: Collection[Union[str, PathLike]], priority: int = 10) -> int:
        """Imports photo files into the database with a designated priority

        :return the number of photos imported"""
        logger = logging.getLogger()
        num_added_photos = num_merged_photos = num_skipped_photos = num_error_photos = 0
        for current_file in tqdm(files):
            if logger.isEnabledFor(logging.DEBUG):
                tqdm.write(f"Importing {current_file}")
            try:
                pf = PhotoFile.from_file(current_file, priority=priority)
                uid = self.find_photo(photo=pf)
                result = self.add_photo(photo=pf, uid=uid)

                if result is not None:
                    if uid is None:
                        num_added_photos += 1
                    else:
                        num_merged_photos += 1
                else:
                    num_skipped_photos += 1
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

    def verify_stored_photos(
            self,
            directory: Union[str, PathLike],
            subdirectory: Union[str, PathLike] = ''
    ) -> int:
        """Check the files stored in directory against checksums in the database

        :return the number of errors found"""
        num_correct_photos = num_incorrect_photos = num_missing_photos = 0
        directory = Path(directory).expanduser().resolve()
        subdirectory = Path(subdirectory)
        if subdirectory.is_absolute():
            raise DatabaseException("Absolute subdirectory not supported")
        abs_subdirectory = directory / subdirectory
        stored_photos = []
        for photos in self.photo_db.values():
            for photo in photos:
                abs_store_path = directory / photo.store_path
                if photo.store_path and abs_store_path.is_relative_to(abs_subdirectory):
                    stored_photos.append(photo)
        for photo in tqdm(stored_photos):
            abs_store_path = directory / photo.store_path
            if not abs_store_path.exists():
                tqdm.write(f"Missing photo: {abs_store_path}")
                num_missing_photos += 1
            elif file_checksum(abs_store_path) == photo.checksum:
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
