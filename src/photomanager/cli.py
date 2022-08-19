#!/usr/bin/env python
from __future__ import annotations

import json
import logging
import sys
from os import PathLike
from typing import Iterable, Optional, Union

import click

from photomanager import version
from photomanager.actions import actions, fileops
from photomanager.database import Database, sizeof_fmt
from photomanager.hasher import DEFAULT_HASH_ALGO, HASH_ALGORITHMS, HashAlgorithm

DEFAULT_DB = "photos.json"


def config_logging(debug: bool = False):
    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(levelname)s:%(name)s: %(message)s",
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
        )


def click_exit(value: int = 0):
    ctx = click.get_current_context()
    ctx.exit(value)


# fmt: off
@click.command("create",
               help="Create a database. Save a new version if it already exists.")
@click.option("--db", type=click.Path(dir_okay=False), required=True,
              default=DEFAULT_DB,
              help="PhotoManager database filepath (.json). "
                   "Add extensions .zst or .gz to compress.")
@click.option("--hash-algorithm",
              type=click.Choice(HASH_ALGORITHMS),
              default=DEFAULT_HASH_ALGO.value,
              help=f"Hash algorithm (default={DEFAULT_HASH_ALGO.value})")
@click.option("--timezone-default", type=str, default="local",
              help="Timezone to use when indexing timezone-naive photos "
                   "(example=\"-0400\", default=\"local\")")
@click.option("--debug", default=False, is_flag=True,
              help="Run in debug mode")
# fmt: on
def _create(
    db: Union[str, PathLike],
    hash_algorithm: str = DEFAULT_HASH_ALGO.value,
    timezone_default: str = "local",
    debug: bool = False,
):
    config_logging(debug=debug)
    try:
        database = Database.from_file(db)
    except FileNotFoundError:
        database = Database()
    database.hash_algorithm = HashAlgorithm(hash_algorithm)
    database.db["timezone_default"] = timezone_default
    database.save(path=db, argv=sys.argv, force=True)


# fmt: off
@click.command("index", help="Index and add items to database")
@click.option("--db", type=click.Path(dir_okay=False),
              help="PhotoManager database filepath (.json). "
                   "Add extensions .zst or .gz to compress.")
@click.option("--source", type=click.Path(file_okay=False),
              help="Directory to index")
@click.option("--file", type=click.Path(dir_okay=False),
              help="File to index")
@click.option("--exclude", multiple=True,
              help="Name patterns to exclude")
@click.option("--skip-existing", default=False, is_flag=True,
              help="Don't index files that are already in the database")
@click.option("--priority", type=int, default=10,
              help="Priority of indexed photos (lower is preferred, default=10)")
@click.option("--timezone-default", type=str, default=None,
              help="Timezone to use when indexing timezone-naive photos "
                   "(example=\"-0400\", default=\"local\")")
@click.option("--hash-algorithm",
              type=click.Choice(HASH_ALGORITHMS),
              default=DEFAULT_HASH_ALGO.value,
              help=f"Hash algorithm to use if no database provided "
                   f"(default={DEFAULT_HASH_ALGO.value})")
@click.option("--storage-type", type=click.Choice(fileops.STORAGE_TYPES), default="HDD",
              help="Class of storage medium (HDD, SSD, RAID)")
@click.option("--debug", default=False, is_flag=True,
              help="Run in debug mode")
@click.option("--dump", default=False, is_flag=True,
              help="Print photo info to stdout")
@click.option("--dry-run", default=False, is_flag=True,
              help="Perform a dry run that makes no changes")
@click.argument("paths", nargs=-1, type=click.Path())
# fmt: on
def _index(
    db: Union[str, PathLike] = None,
    source: Optional[Union[str, PathLike]] = None,
    file: Optional[Union[str, PathLike]] = None,
    paths: Iterable[Union[str, PathLike]] = tuple(),
    exclude: Iterable[str] = tuple(),
    skip_existing: bool = False,
    debug: bool = False,
    dry_run: bool = False,
    dump: bool = False,
    priority: int = 10,
    timezone_default: Optional[str] = None,
    hash_algorithm: str = DEFAULT_HASH_ALGO.value,
    storage_type: str = "HDD",
):
    if not source and not file and not paths:
        print("Nothing to index")
        print(click.get_current_context().get_help())
        click_exit(1)
    config_logging(debug=debug)
    if db is not None:
        database = Database.from_file(db, create_new=True)
        skip_existing = set(database.sources) if skip_existing else set()
    else:
        database = Database()
        skip_existing = set()
        database.hash_algorithm = HashAlgorithm(hash_algorithm)
    filtered_files = fileops.list_files(
        source=source,
        file=file,
        exclude=exclude,
        exclude_files=skip_existing,
        paths=paths,
    )
    index_result = actions.index(
        database=database,
        files=filtered_files,
        priority=priority,
        timezone_default=timezone_default,
        storage_type=storage_type,
    )
    if dump:
        photos = index_result["photos"]
        result = {}
        for filename, photo in zip(filtered_files, photos):
            result[filename] = photo.to_dict()
        print(json.dumps(result, indent=2))
    if db is not None and not dry_run:
        database.save(path=db, argv=sys.argv)
    click_exit(1 if index_result["num_error_photos"] else 0)


# fmt: off
@click.command("collect", help="Collect highest-priority items into storage")
@click.option("--db", type=click.Path(dir_okay=False, exists=True), required=True,
              default=DEFAULT_DB, help="PhotoManager database path")
@click.option("--destination", type=click.Path(file_okay=False), required=True,
              help="Photo storage base directory")
@click.option("--debug", default=False, is_flag=True,
              help="Run in debug mode")
@click.option("--dry-run", default=False, is_flag=True,
              help="Perform a dry run that makes no changes")
@click.option("--collect-db", default=False, is_flag=True,
              help="Also save the database within destination")
# fmt: on
def _collect(
    db: Union[str, PathLike],
    destination: Union[str, PathLike],
    debug: bool = False,
    dry_run: bool = False,
    collect_db: bool = False,
):
    config_logging(debug=debug)
    database = Database.from_file(db)
    collect_result = actions.collect(
        database=database, destination=destination, dry_run=dry_run
    )
    if not dry_run:
        database.save(
            path=db, argv=sys.argv, collect_db=collect_db, destination=destination
        )
    click_exit(
        1
        if collect_result["num_missed_photos"] or collect_result["num_error_photos"]
        else 0
    )


# fmt: off
@click.command("import", help="Index items and collect to directory")
@click.option("--db", type=click.Path(dir_okay=False), required=True,
              default=DEFAULT_DB,
              help="PhotoManager database filepath (.json). "
                   "Add extensions .zst or .gz to compress.")
@click.option("--destination", type=click.Path(file_okay=False), required=True,
              help="Photo storage base directory")
@click.option("--source", type=click.Path(file_okay=False),
              help="Directory to index")
@click.option("--file", type=click.Path(dir_okay=False),
              help="File to index")
@click.option("--exclude", multiple=True,
              help="Name patterns to exclude")
@click.option("--skip-existing", default=False, is_flag=True,
              help="Don't index files that are already in the database")
@click.option("--priority", type=int, default=10,
              help="Priority of indexed photos (lower is preferred, default=10)")
@click.option("--timezone-default", type=str, default=None,
              help="Timezone to use when indexing timezone-naive photos "
                   "(example=\"-0400\", default=\"local\")")
@click.option("--storage-type", type=click.Choice(fileops.STORAGE_TYPES), default="HDD",
              help="Class of storage medium (HDD, SSD, RAID)")
@click.option("--debug", default=False, is_flag=True,
              help="Run in debug mode")
@click.option("--dry-run", default=False, is_flag=True,
              help="Perform a dry run that makes no changes")
@click.option("--collect-db", default=False, is_flag=True,
              help="Also save the database within destination")
@click.argument("paths", nargs=-1, type=click.Path())
# fmt: on
def _import(
    db: Union[str, PathLike],
    destination: Union[str, PathLike],
    source: Optional[Union[str, PathLike]] = None,
    file: Optional[Union[str, PathLike]] = None,
    paths: Iterable[Union[str, PathLike]] = tuple(),
    exclude: Iterable[str] = tuple(),
    skip_existing: bool = False,
    debug: bool = False,
    dry_run: bool = False,
    priority: int = 10,
    timezone_default: Optional[str] = None,
    storage_type: str = "HDD",
    collect_db: bool = False,
):
    config_logging(debug=debug)
    database = Database.from_file(db, create_new=True)
    skip_existing = set(database.sources) if skip_existing else set()
    filtered_files = fileops.list_files(
        source=source,
        file=file,
        exclude=exclude,
        exclude_files=skip_existing,
        paths=paths,
    )
    index_result = actions.index(
        database=database,
        files=filtered_files,
        priority=priority,
        timezone_default=timezone_default,
        storage_type=storage_type,
    )
    collect_result = actions.collect(
        database=database,
        destination=destination,
        dry_run=dry_run,
        filter_uids=index_result["changed_uids"] if skip_existing else None,
    )
    if not dry_run:
        database.save(
            path=db, argv=sys.argv, collect_db=collect_db, destination=destination
        )
    click_exit(
        1
        if index_result["num_error_photos"]
        or collect_result["num_missed_photos"]
        or collect_result["num_error_photos"]
        else 0
    )


# fmt: off
@click.command("clean", help="Remove lower-priority alternatives of stored items")
@click.option("--db", type=click.Path(dir_okay=False, exists=True), required=True,
              default=DEFAULT_DB, help="PhotoManager database path")
@click.option("--destination", type=click.Path(file_okay=False), required=True,
              help="Photo storage base directory")
@click.option("--subdir", type=str, default="",
              help="Remove only items within subdirectory")
@click.option("--debug", default=False, is_flag=True,
              help="Run in debug mode")
@click.option("--dry-run", default=False, is_flag=True,
              help="Perform a dry run that makes no changes")
# fmt: on
def _clean(
    db: Union[str, PathLike],
    destination: Union[str, PathLike],
    subdir: Union[str, PathLike] = "",
    debug: bool = False,
    dry_run: bool = False,
):
    config_logging(debug=debug)
    database = Database.from_file(db)
    result = actions.clean(
        database=database,
        destination=destination,
        subdir=subdir,
        dry_run=dry_run,
    )
    if not dry_run:
        database.save(path=db, argv=sys.argv)
    click_exit(1 if result["num_missing_photos"] else 0)


# fmt: off
@click.command("verify", help="Verify checksums of stored items")
@click.option("--db", type=click.Path(dir_okay=False, exists=True), required=True,
              default=DEFAULT_DB, help="PhotoManager database path")
@click.option("--destination", type=click.Path(file_okay=False), required=True,
              help="Photo storage base directory")
@click.option("--subdir", type=str, default="",
              help="Verify only items within subdirectory")
@click.option("--storage-type", type=click.Choice(fileops.STORAGE_TYPES), default="HDD",
              help="Class of storage medium (HDD, SSD, RAID)")
@click.option("--random-fraction", type=float, default=None,
              help="Verify a randomly sampled fraction of the photos")
@click.option("--debug", default=False, is_flag=True,
              help="Run in debug mode")
# fmt: on
def _verify(
    db: Union[str, PathLike],
    destination: Union[str, PathLike],
    subdir: Union[str, PathLike] = "",
    storage_type: str = "HDD",
    random_fraction: Optional[float] = None,
    debug: bool = False,
):
    config_logging(debug=debug)
    database = Database.from_file(db)
    result = actions.verify(
        database=database,
        directory=destination,
        subdir=subdir,
        storage_type=storage_type,
        random_fraction=random_fraction,
    )
    click_exit(
        1 if result["num_incorrect_photos"] or result["num_missing_photos"] else 0
    )


# fmt: off
@click.command("stats", help="Get database statistics")
@click.option("--db", type=click.Path(dir_okay=False, exists=True), required=True,
              default=DEFAULT_DB, help="PhotoManager database path")
# fmt: on
def _stats(db: Union[str, PathLike]):
    config_logging()
    database = Database.from_file(db)
    num_uids, num_photos, num_stored_photos, total_file_size = database.get_stats()
    print(f"Total items:        {num_photos}")
    print(f"Total unique items: {num_uids}")
    print(f"Total stored items: {num_stored_photos}")
    print(f"Total file size:    {sizeof_fmt(total_file_size)}")


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


# fmt: off
@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=version, prog_name="photomanager",
                      message="%(prog)s %(version)s")
# fmt: on
def main():
    pass


main.add_command(_create)
main.add_command(_index)
main.add_command(_collect)
main.add_command(_import)
main.add_command(_clean)
main.add_command(_verify)
main.add_command(_stats)


def _init():
    if __name__ == "__main__":
        main()


_init()
