from __future__ import annotations

import sys
from os import PathLike
from pathlib import Path
from typing import Union, Optional
from collections.abc import Iterable
import logging
import random

from tqdm import tqdm

from photomanager.database import Database, tz_str_to_tzinfo, sizeof_fmt
from photomanager.actions import fileops


def index(
    database: Database,
    files: Iterable[Union[str, PathLike]],
    priority: int = 10,
    timezone_default: Optional[str] = None,
    storage_type: str = "HDD",
) -> dict[str, int]:
    logger = logging.getLogger(__name__)
    tz_default = (
        tz_str_to_tzinfo(timezone_default)
        if timezone_default is not None
        else database.timezone_default
    )
    photos = fileops.index_photos(
        files=files,
        priority=priority,
        storage_type=storage_type,
        hash_algorithm=database.hash_algorithm,
        tz_default=tz_default,
    )
    num_error_photos = sum(pf is None for pf in photos)
    num_added_photos, num_merged_photos, num_skipped_photos = database.add_photos(
        photos=(pf for pf in photos if pf is not None),
    )
    logger.info(f"Indexed {num_added_photos+num_merged_photos}/{len(photos)} items")
    logger.info(
        f"Added {num_added_photos} new items and merged {num_merged_photos} items"
    )
    if num_skipped_photos:
        logger.info(f"Skipped {num_skipped_photos} items")
    if num_error_photos:  # pragma: no cover
        logger.info(f"Encountered an error on {num_error_photos} items")
    return dict(
        num_added_photos=num_added_photos,
        num_merged_photos=num_merged_photos,
        num_skipped_photos=num_skipped_photos,
        num_error_photos=num_error_photos,
    )


def collect(
    database: Database,
    destination: Union[str, PathLike],
    dry_run: bool = False,
) -> dict[str, int]:
    logger = logging.getLogger(__name__)
    (
        photos_to_copy,
        (num_copied_photos, num_added_photos, num_missed_photos, num_stored_photos),
    ) = database.get_photos_to_collect(destination)
    total_copied_photos, total_copy_size, num_error_photos = fileops.copy_photos(
        destination, photos_to_copy, dry_run=dry_run
    )
    logger.info(
        f"{'Would copy' if dry_run else 'Copied'} {total_copied_photos} items, "
        f"total size: {sizeof_fmt(total_copy_size)}: "
        f"{num_added_photos} new items and {num_copied_photos} "
        f"items marked as stored elsewhere"
    )
    if num_stored_photos or num_missed_photos:
        logger.info(
            f"Skipped {num_stored_photos} items already stored "
            f"and {num_missed_photos} missing items"
        )
    if num_error_photos:  # pragma: no cover
        logger.warning(f"Encountered errors copying {num_error_photos} items")
    return dict(
        num_copied_photos=num_copied_photos,
        num_added_photos=num_added_photos,
        num_missed_photos=num_missed_photos,
        num_stored_photos=num_stored_photos,
        total_copied_photos=total_copied_photos,
        total_copy_size=total_copy_size,
        num_error_photos=num_error_photos,
    )


def clean(
    database: Database,
    destination: Union[str, PathLike],
    subdir: Union[str, PathLike] = "",
    dry_run: bool = False,
) -> dict[str, int]:
    logger = logging.getLogger(__name__)
    photos_to_remove = database.get_photos_to_remove(
        destination, subdirectory=subdir, dry_run=dry_run
    )
    total_file_size = sum(pf.fsz for pf in photos_to_remove)
    logger.info(f"Identified {len(photos_to_remove)} lower-priority items for removal")
    logger.info(f"Total file size: {sizeof_fmt(total_file_size)}")
    num_removed_photos, num_missing_photos = fileops.remove_photos(
        destination, photos_to_remove, dry_run=dry_run
    )
    logger.info(
        f"{'Found' if dry_run else 'Removed'} {num_removed_photos} items "
        f"and skipped {num_missing_photos} missing items"
    )
    return dict(
        num_removed_photos=num_removed_photos,
        total_file_size=total_file_size,
        num_missing_photos=num_missing_photos,
    )


def verify(
    database: Database,
    directory: Union[str, PathLike],
    subdir: Union[str, PathLike] = "",
    storage_type: str = "HDD",
    random_fraction: Optional[float] = None,
) -> dict[str, int]:
    """
    Check the files stored in directory against checksums in the database.

    :param database: the Database
    :param directory: the photo storage directory
    :param subdir: verify only photos within subdirectory
    :param storage_type: the type of media the photos are stored on
        (uses async if SSD)
    :param random_fraction: verify a randomly sampled fraction of the photos
    :return: the number of errors found
    """
    logger = logging.getLogger(__name__)
    num_correct_photos = num_incorrect_photos = num_missing_photos = 0
    total_file_size = 0
    destination = Path(directory).expanduser().resolve()
    stored_photos = database.get_stored_photos(subdir)
    if random_fraction is not None:
        n = len(stored_photos)
        k = max(min(round(random_fraction * n), n), 0)
        stored_photos = random.sample(stored_photos, k=k)
    logger.info(f"Verifying {len(stored_photos)} items")
    logger.info(f"Total file size: {sizeof_fmt(total_file_size)}")

    logger.info("Collecting media hashes")
    checksum_cache = fileops.hash_stored_photos(
        photos=stored_photos,
        directory=directory,
        hash_algorithm=database.hash_algorithm,
        storage_type=storage_type,
    )

    for photo in tqdm(stored_photos):
        abs_store_path = str(destination / photo.sto)
        if abs_store_path not in checksum_cache:
            tqdm.write(f"Missing photo: {abs_store_path}", file=sys.stderr)
            num_missing_photos += 1
        elif checksum_cache[abs_store_path] == photo.chk:
            num_correct_photos += 1
        else:
            tqdm.write(f"Incorrect checksum: {abs_store_path}", file=sys.stderr)
            num_incorrect_photos += 1

    logger.info(
        f"Checked "
        f"{num_correct_photos+num_incorrect_photos+num_missing_photos} "
        f"items"
    )
    if num_incorrect_photos or num_missing_photos:
        logger.warning(
            f"Found {num_incorrect_photos} incorrect and "
            f"{num_missing_photos} missing items"
        )
    else:
        logger.info("No errors found")
    return dict(
        num_correct_photos=num_correct_photos,
        num_incorrect_photos=num_incorrect_photos,
        num_missing_photos=num_missing_photos,
    )
