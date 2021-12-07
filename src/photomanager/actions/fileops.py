from __future__ import annotations

import sys
from os import PathLike, cpu_count, makedirs, chmod, remove
import stat
from pathlib import Path
from datetime import tzinfo
import shutil
import logging
import traceback
from typing import Union, Optional
from collections.abc import Collection, Iterable
import re

from tqdm import tqdm
import click

from photomanager.hasher import (
    AsyncFileHasher,
    DEFAULT_HASH_ALGO,
    HashAlgorithm,
)
from photomanager.photofile import PhotoFile, extensions
from photomanager.pyexiftool import ExifTool, AsyncExifTool
from photomanager.database import sizeof_fmt


STORAGE_TYPES = ("HDD", "SSD", "RAID")


def list_files(
    source: Optional[Union[str, PathLike]] = None,
    file: Optional[Union[str, PathLike]] = None,
    paths: Iterable[Union[str, PathLike]] = tuple(),
    exclude: Iterable[str] = tuple(),
    exclude_files: Iterable[Union[str, PathLike]] = tuple(),
) -> dict[str, None]:
    """
    List all files in sources, excluding regex patterns.

    :param source: Directory to list. If `-`, read directories from stdin.
    :param file: File to list. If `-`, read files from stdin.
    :param paths: Paths (directories or files) to list.
    :param exclude: Regex patterns to exclude.
    :param exclude_files: File paths to exclude
    :return: A dictionary with paths as keys.
    """
    logger = logging.getLogger(__name__)
    paths = {Path(p).expanduser().resolve(): None for p in paths}
    if source == "-":
        with click.open_file("-", "r") as f:
            sources: str = f.read()
        paths.update(
            {
                Path(p).expanduser().resolve(): None
                for p in sources.splitlines(keepends=False)
            }
        )
    elif source:
        paths[Path(source).expanduser().resolve()] = None

    files = {}
    if file == "-":
        with click.open_file("-", "r") as f:
            sources: str = f.read()
        files.update(
            {
                Path(p).expanduser().resolve(): None
                for p in sources.splitlines(keepends=False)
            }
        )
    elif file:
        files[Path(file).expanduser().resolve()] = None
    for path in paths:
        for p in path.glob("**/*.*"):
            files[p] = None

    exclude_files = {Path(f).expanduser().resolve() for f in exclude_files}
    filtered_files = {}
    exclude_patterns = [re.compile(pat) for pat in set(exclude)]
    skipped_extensions = set()
    for p in files:
        if p in exclude_files:
            continue
        if p.suffix.lower().lstrip(".") not in extensions:
            skipped_extensions.add(p.suffix.lower().lstrip("."))
            continue
        if any(regex.search(str(p)) for regex in exclude_patterns):
            continue
        filtered_files[str(p)] = None
    if skipped_extensions:
        logger.info(f"Skipped extensions: {skipped_extensions}")

    return filtered_files


def index_photos(
    files: Iterable[Union[str, PathLike]],
    priority: int = 10,
    hash_algorithm: HashAlgorithm = DEFAULT_HASH_ALGO,
    tz_default: Optional[tzinfo] = None,
    storage_type: str = "HDD",
) -> list[Optional[PhotoFile]]:
    """
    Indexes photo files

    :param files: the photo file paths to index
    :param priority: the photos' priority
    :param hash_algorithm: The hashing algorithm to use for file checksums
    :param tz_default: The time zone to use if none is set
        (defaults to local time)
    :param storage_type: the storage type being indexed (uses more async if SSD)
    :return: a list of PhotoFiles, with None entries for errors
    """
    logger = logging.getLogger(__name__)
    if storage_type in ("SSD", "RAID"):
        async_hashes = True
        async_exif = cpu_count()
    else:
        # concurrent reads of sequential files can lead to thrashing
        async_hashes = False
        # exiftool is partially CPU-bound and benefits from async
        async_exif = min(4, cpu_count())
    logger.info("Collecting media hashes")
    checksum_cache = AsyncFileHasher(
        algorithm=hash_algorithm, use_async=async_hashes
    ).check_files(files, pbar_unit="B")
    logger.info("Collecting media dates and times")
    datetime_cache = AsyncExifTool(num_workers=async_exif).get_best_datetime_batch(
        files
    )

    logger.info("Indexing media")
    photos = []
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
                algorithm=hash_algorithm,
                tz_default=tz_default,
                priority=priority,
            )
            photos.append(pf)
        except Exception as e:
            tqdm.write(f"Error indexing {current_file}", file=sys.stderr)
            tb_str = "".join(
                traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
            )
            tqdm.write(tb_str, file=sys.stderr)
            photos.append(None)
    exiftool.terminate()
    return photos


def copy_photos(
    directory: Union[str, PathLike],
    photos: Collection[tuple[PhotoFile, Optional[str]]],
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """
    Copies photos into a directory.

    Updates the store_path for photos newly stored. Store_paths are
    relative to the storage directory.
    Stored photos have permissions set to read-only for all.

    :param directory: the photo storage directory
    :param photos: PhotoFiles to copy, and optionally their destination
    :param dry_run: if True, do not copy photos
    """
    logger = logging.getLogger(__name__)
    directory = Path(directory).expanduser().resolve()
    num_copied_photos = num_error_photos = total_copy_size = 0
    estimated_copy_size = sum(photo.fsz for photo, _ in photos)
    logger.info(
        f"{'Would copy' if dry_run else 'Copying'} {len(photos)} items, "
        f"estimated size: {sizeof_fmt(estimated_copy_size)}"
    )
    p_bar = tqdm(
        total=estimated_copy_size, unit="B", unit_scale=True, unit_divisor=1024
    )
    for photo, rel_store_path in photos:
        if rel_store_path is None:
            abs_store_path = directory / photo.sto
        else:
            abs_store_path = directory / rel_store_path
        if logger.isEnabledFor(logging.DEBUG):
            tqdm.write(
                f"{'Would copy' if dry_run else 'Copying'}: {photo.src} "
                f"to {abs_store_path}"
            )
        try:
            if not dry_run:
                makedirs(abs_store_path.parent, exist_ok=True)
                shutil.copy2(photo.src, abs_store_path)
                chmod(abs_store_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                if rel_store_path is not None:
                    photo.sto = rel_store_path
        except Exception as e:
            tqdm.write(
                f"Error copying {photo.src} to {abs_store_path}", file=sys.stderr
            )
            tb_str = "".join(
                traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
            )
            tqdm.write(tb_str, file=sys.stderr)
            num_error_photos += 1
        else:
            num_copied_photos += 1
            total_copy_size += photo.fsz
        p_bar.update(photo.fsz)
    p_bar.close()
    return num_copied_photos, total_copy_size, num_error_photos


def remove_photos(
    directory: Union[str, PathLike],
    photos: Iterable[PhotoFile],
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Removes photos stored in directory

    :param directory the photo storage directory
    :param photos: PhotoFiles to remove
    :param dry_run: if True, do not remove photos
    :return: the number of photos removed and missing
    """
    logger = logging.getLogger(__name__)
    directory = Path(directory).expanduser().resolve()
    num_removed_photos = num_missing_photos = 0
    for photo in tqdm(photos):
        abs_store_path = directory / photo.sto
        if abs_store_path.exists():
            if logger.isEnabledFor(logging.DEBUG):
                tqdm.write(
                    f"{'Would remove' if dry_run else 'Removing'}: {abs_store_path}"
                )
            if not dry_run:
                remove(abs_store_path)
                photo.sto = ""
            num_removed_photos += 1
        else:
            if logger.isEnabledFor(logging.DEBUG):
                tqdm.write(f"Missing photo: {abs_store_path}")
            num_missing_photos += 1
    return num_removed_photos, num_missing_photos


def hash_stored_photos(
    photos: Iterable[PhotoFile],
    directory: Union[str, PathLike],
    hash_algorithm: HashAlgorithm = DEFAULT_HASH_ALGO,
    storage_type: str = "HDD",
) -> dict[str, str]:
    """
    Checks the hashes of stored PhotoFiles

    :param directory the photo storage directory
    :param photos: PhotoFiles to remove
    :param hash_algorithm: the HashAlgorithm to use
    :param storage_type: the type of media the photos are stored on
            (uses async if SSD or RAID)
    :return: A dict from filepath (absolute) to checksum
    """
    directory = Path(directory).expanduser().resolve()
    if storage_type in ("SSD", "RAID"):
        async_hashes = True
    else:
        # concurrent reads of sequential files can lead to thrashing
        async_hashes = False
    files, sizes = [], []
    for photo in photos:
        abs_store_path = directory / photo.sto
        if abs_store_path.exists():
            files.append(str(abs_store_path))
            sizes.append(photo.fsz)
    checksum_cache = AsyncFileHasher(
        algorithm=hash_algorithm,
        use_async=async_hashes,
    ).check_files(files, pbar_unit="B", file_sizes=sizes)
    return checksum_cache
