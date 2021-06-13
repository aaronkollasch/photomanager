from __future__ import annotations

from os import PathLike
from os.path import getsize
from datetime import datetime, tzinfo, timezone, timedelta
from dataclasses import dataclass, asdict, fields
from typing import Union, Optional, Type, TypeVar

from photomanager.pyexiftool import ExifTool
from photomanager.hasher import file_checksum, DEFAULT_HASH_ALGO, HashAlgorithm

PF = TypeVar("PF", bound="PhotoFile")
local_tzoffset = datetime.now().astimezone().utcoffset().total_seconds()
NAME_MAP_ENC: dict[str, str] = {
    "checksum": "chk",
    "source_path": "src",
    "datetime": "dt",
    "timestamp": "ts",
    "file_size": "fsz",
    "store_path": "sto",
    "priority": "prio",
    "tz_offset": "tzo",
}
NAME_MAP_DEC: dict[str, str] = {v: k for k, v in NAME_MAP_ENC.items()}


@dataclass
class PhotoFile:
    """A dataclass describing a photo or other media file

    Attributes:
        :chk (bytes): checksum of photo file
        :src (str): Absolute path where photo was found
        :dt (str): Datetime string for best estimated creation date (original)
        :ts (float): POSIX timestamp of best estimated creation date (derived)
        :fsz (int): Photo file size, in bytes
        :sto (str): Relative path where photo is stored, empty if not stored
        :prio (int): Photo priority (lower is preferred)
        :tzo (float): local time zone offset
    """

    chk: bytes
    src: str
    dt: str
    ts: float
    fsz: int
    sto: str = ""
    prio: int = 10
    tzo: float = None

    @property
    def __dict__(self):
        d = {f.name: getattr(self, f.name) for f in fields(self)}
        d["chk"] = d["chk"].hex()
        return d

    @property
    def local_datetime(self):
        tz = timezone(timedelta(seconds=self.tzo)) if self.tzo is not None else None
        return datetime.fromtimestamp(self.ts).astimezone(tz)

    @classmethod
    def from_file(
        cls: Type[PF],
        source_path: Union[str, PathLike],
        algorithm: HashAlgorithm = DEFAULT_HASH_ALGO,
        tz_default: Optional[tzinfo] = None,
        priority: int = 10,
    ) -> PF:
        """Create a PhotoFile for a given file

        :param source_path: The path to the file
        :param algorithm: The hashing algorithm to use
        :param tz_default: The time zone to use if none is set
            (defaults to local time)
        :param priority: The photo's priority
        """
        photo_hash = file_checksum(source_path, algorithm)
        dt_str = get_media_datetime(source_path)
        dt = datetime_str_to_object(dt_str, tz_default=tz_default)
        tz = dt.utcoffset().total_seconds() if dt.tzinfo is not None else local_tzoffset
        timestamp = dt.timestamp()
        file_size = getsize(source_path)
        return cls(
            chk=photo_hash,
            src=str(source_path),
            dt=dt_str,
            ts=timestamp,
            fsz=file_size,
            sto="",
            prio=priority,
            tzo=tz,
        )

    @classmethod
    def from_file_cached(
        cls: Type[PF],
        source_path: str,
        checksum_cache: dict[str, bytes],
        datetime_cache: dict[str, str],
        algorithm: HashAlgorithm = DEFAULT_HASH_ALGO,
        tz_default: Optional[tzinfo] = None,
        priority: int = 10,
    ) -> PF:
        """Create a PhotoFile for a given file

        If source_path is in the checksum and datetime caches, uses the cached value
        instead of reading from the file.

        :param source_path: The path to the file
        :param checksum_cache: A mapping of source paths to known checksums
        :param datetime_cache: A mapping of source paths to datetime strings
        :param algorithm: The hashing algorithm to use for new checksums
        :param tz_default: The time zone to use if none is set
            (defaults to local time)
        :param priority: The photo's priority
        """
        photo_hash = (
            checksum_cache[source_path]
            if source_path in checksum_cache
            else file_checksum(source_path, algorithm)
        )
        dt_str = (
            datetime_cache[source_path]
            if source_path in datetime_cache
            else get_media_datetime(source_path)
        )
        dt = datetime_str_to_object(dt_str, tz_default=tz_default)
        tz = dt.utcoffset().total_seconds() if dt.tzinfo else None
        timestamp = dt.timestamp()
        file_size = getsize(source_path)
        return cls(
            chk=photo_hash,
            src=str(source_path),
            dt=dt_str,
            ts=timestamp,
            fsz=file_size,
            sto="",
            prio=priority,
            tzo=tz,
        )

    @classmethod
    def from_json_dict(cls: Type[PF], d: dict) -> PF:
        d["chk"] = bytes.fromhex(d["chk"])
        return cls.from_dict(d)

    @classmethod
    def from_dict(cls: Type[PF], d: dict) -> PF:
        return cls(**d)

    def to_dict(self) -> dict:
        return asdict(self)


def datetime_str_to_object(ts_str: str, tz_default: tzinfo = None) -> datetime:
    """Parses a datetime string into a datetime object"""
    dt = None
    if "." in ts_str:
        for fmt in ("%Y:%m:%d %H:%M:%S.%f%z", "%Y:%m:%d %H:%M:%S.%f"):
            try:
                dt = datetime.strptime(ts_str, fmt)
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
                dt = datetime.strptime(ts_str, fmt)
            except ValueError:
                pass
    if dt is not None:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz_default)
        return dt
    raise ValueError(f"Could not parse datetime str: {repr(ts_str)}")


def get_media_datetime(path: Union[str, PathLike]) -> str:
    """Gets the best known datetime string for a file"""
    return ExifTool().get_best_datetime(path)
