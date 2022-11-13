from __future__ import annotations

import logging
from os import PathLike, rename
from os.path import exists
from pathlib import Path
from typing import Optional, Union

from tqdm import tqdm

from photomanager.database import Database, sizeof_fmt
from photomanager.hasher import HashAlgorithm, file_checksum


def make_hash_map(
    database: Database,
    new_algo: HashAlgorithm,
    hash_map: Optional[dict[str, str]] = None,
    destination: Optional[Union[str, PathLike]] = None,
) -> dict[str, str]:  # pragma: no cover
    """Make a map of file checksums in order to migrate hashing algorithms.

    Checks source file hashes using the old algorithm to make sure the new hashes
    are correct. If the source has an incorrect hash, does not map checksum and
    instead sets the mapped checksum to '{old_checksum}:{old_algorithm}'

    Note: This method is not accessed by the CLI or covered by testing
    and is intended to be used interactively, i.e. in a Jupyter notebook
    or other environment, with spot checking of the output hash map.

    :param database: the Database
    :param new_algo: the new algorithm to use
    :param hash_map: a map from old hashes to new hashes; will be updated with
        new mappings as they are found
    :param destination: the library storage destination
    :return: the hash map
    """
    if hash_map is None:
        hash_map = {}
    old_algo = database.hash_algorithm
    print(f"Converting {old_algo} to {new_algo}")
    num_correct_photos = (
        num_incorrect_photos
    ) = num_missing_photos = num_skipped_photos = 0
    all_photos = [photo for photos in database.photo_db.values() for photo in photos]
    for photo in tqdm(all_photos):
        if photo.chk in hash_map:
            num_skipped_photos += 1
        elif exists(photo.src):
            if photo.chk == file_checksum(photo.src, old_algo):
                hash_map[photo.chk] = file_checksum(photo.src, new_algo)
                num_correct_photos += 1
            else:
                tqdm.write(f"Incorrect checksum: {photo.src}")
                hash_map[photo.chk] = photo.chk + f":{old_algo}"
                num_incorrect_photos += 1
        elif destination:
            sto_path = Path(destination).expanduser().resolve() / photo.sto
            if exists(sto_path) and photo.chk == file_checksum(sto_path, old_algo):
                hash_map[photo.chk] = file_checksum(sto_path, new_algo)
                num_correct_photos += 1
            else:
                num_missing_photos += 1
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
    database: Database,
    new_algo: HashAlgorithm,
    hash_map: dict[str, str],
    map_all: bool = False,
) -> Optional[int]:  # pragma: no cover
    """Map the database's checksums to a new algorithm.

    If a checksum cannot be mapped, it is appended by ':{old_algorithm}'
    to denote that it was made with a different algorithm

    Note: This method is not accessed by the CLI or covered by testing
    and is intended to be used interactively, i.e. in a Jupyter notebook
    or other environment, with spot checking of the newly mapped hashes.

    :param database: the Database
    :param new_algo: the hashing algorithm used to make hash_map
    :param hash_map: the map from old hashes to new hashes
    :param map_all: set to True to enforce that all hashes must be in hash_map
        before mapping starts
    :return: the number of hashes not mapped
    """
    num_correct_photos = num_skipped_photos = 0
    old_algo = database.hash_algorithm
    all_photos = [photo for photos in database.photo_db.values() for photo in photos]
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
    database.hash_algorithm = new_algo
    print(f"Mapped {num_correct_photos} items")
    if num_skipped_photos:
        print(f"Did not map {num_skipped_photos} items")
    return num_skipped_photos


def update_stored_filename_hashes(
    database: Database,
    destination: Union[str, PathLike],
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

    :param database: the Database
    :param destination: the photo storage directory
    :param verify: if True, verify that file checksums match
    :param dry_run: if True, perform a dry run and do not move photos
    :return: the mapping of files moved
    """
    num_correct_photos = (
        num_skipped_photos
    ) = num_incorrect_photos = num_missing_photos = 0
    destination = Path(destination).expanduser().resolve()
    stored_photos = [
        photo for photos in database.photo_db.values() for photo in photos if photo.sto
    ]
    total_file_size = sum(photo.fsz for photo in stored_photos)
    print(f"Updating {len(stored_photos)} filename hashes")
    print(f"Total file size: {sizeof_fmt(total_file_size)}")
    logger = logging.getLogger()
    file_map = {}
    for photo in tqdm(stored_photos):
        abs_store_path = destination / photo.sto
        new_store_path = f"{photo.sto[:32]}{photo.chk[:7]}{photo.sto[39:]}"
        new_abs_store_path = destination / new_store_path
        if new_abs_store_path.exists():
            num_skipped_photos += 1
        elif not abs_store_path.exists():
            tqdm.write(f"Missing photo: {abs_store_path}")
            num_missing_photos += 1
        elif photo.sto[32:39] == photo.chk[:7]:
            num_skipped_photos += 1
        elif (
            not verify
            or file_checksum(abs_store_path, database.hash_algorithm) == photo.chk
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
