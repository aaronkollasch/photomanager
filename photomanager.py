#!/usr/bin/env python
from __future__ import annotations
import sys
from pathlib import Path
import shlex
import re
import logging
import click
from database import Database, DEFAULT_HASH_ALGO


photo_extensions = {
    'jpeg', 'jpg', 'png', 'apng', 'gif', 'nef', 'cr2', 'orf', 'tif', 'tiff', 'ico', 'bmp', 'dng', 'arw', 'rw2',
    'heic', 'avif', 'heif', 'heics', 'heifs', 'avics', 'avci', 'avcs', 'mng', 'webp', 'psd', 'jp2', 'psb',
}
video_extensions = {
    'mov', 'mp4', 'm4v', 'avi', 'mpg', 'mpeg', 'avchd', 'mts', 'ts', 'm2ts', '3gp', 'gifv', 'mkv', 'asf', 'ogg', 'webm',
    'flv', '3g2', 'svi', 'mpv'
}
audio_extensions = {
    'm4a', 'ogg', 'aiff', 'wav', 'flac', 'caf', 'mp3',
}
extensions = photo_extensions | video_extensions | audio_extensions


def config_logging(debug=False):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)


@click.command('create', help='Create an empty database')
@click.option('--db', type=click.Path(dir_okay=False), required=True, default='./photos.json',
              help='PhotoManager database filepath (.json). Add extensions .zst or .gz to compress.')
@click.option('--hash-algorithm', type=str, default=DEFAULT_HASH_ALGO,
              help=f'Hash algorithm (default={DEFAULT_HASH_ALGO})')
def _create(db, hash_algorithm=DEFAULT_HASH_ALGO):
    database = Database()
    database.hash_algorithm = hash_algorithm
    database.add_command(shlex.join(sys.argv))
    database.to_file(db)


@click.command('import', help='Find and add items to database')
@click.option('--db', type=click.Path(dir_okay=False), required=True, default='./photos.json',
              help='PhotoManager database filepath (.json). Add extensions .zst or .gz to compress.')
@click.option('--source', type=click.Path(file_okay=False),
              help='Directory to import')
@click.option('--file', type=click.Path(dir_okay=False),
              help='File to import')
@click.option('--exclude', multiple=True,
              help='Name patterns to exclude')
@click.option('--priority', type=int, default=10,
              help='Priority of imported photos (lower is preferred, default=10)')
@click.option('--storage-type', type=str, default='HDD',
              help='Class of storage medium (HDD, SSD, RAID)')
@click.option('--debug', default=False, is_flag=True,
              help='Run in debug mode')
@click.argument('paths', nargs=-1, type=click.Path())
def _import(db, source, file, exclude, paths, debug=False, priority=10, storage_type='HDD'):
    if not source and not file and not paths:
        print("Nothing to import")
        sys.exit(1)
    config_logging(debug=debug)
    database = Database.from_file(db)

    paths = {Path(p).expanduser().resolve(): None for p in paths}
    if source == '-':
        with click.open_file('-', 'r') as f:
            sources: str = f.read()
        paths.update({Path(p).expanduser().resolve(): None for p in sources.splitlines(keepends=False)})
    elif source:
        paths[Path(source).expanduser().resolve()] = None

    files = {}
    if file == '-':
        with click.open_file('-', 'r') as f:
            sources: str = f.read()
        files.update({Path(p).expanduser().resolve(): None for p in sources.splitlines(keepends=False)})
    elif file:
        files[Path(file).expanduser().resolve()] = None
    for path in paths:
        for p in path.glob('**/*.*'):
            files[p] = None

    filtered_files = {}
    exclude_patterns = [re.compile(pat) for pat in set(exclude)]
    skipped_extensions = set()
    for p in files:
        if p.suffix.lower().lstrip('.') not in extensions:
            skipped_extensions.add(p.suffix.lower().lstrip('.'))
            continue
        if any(regex.search(str(p)) for regex in exclude_patterns):
            continue
        filtered_files[str(p)] = None
    if skipped_extensions:
        print(f"Skipped extensions: {skipped_extensions}")

    database.import_photos(files=filtered_files, priority=priority, storage_type=storage_type)
    database.add_command(shlex.join(sys.argv))
    database.to_file(db)


@click.command('collect', help='Collect highest-priority items into storage')
@click.option('--db', type=click.Path(dir_okay=False), required=True,
              default='./photos.json', help='PhotoManager database path')
@click.option('--destination', type=click.Path(file_okay=False), required=True,
              help='Photo storage base directory')
@click.option('--debug', default=False, is_flag=True,
              help='Run in debug mode')
@click.option('--dry-run', default=False, is_flag=True,
              help='Perform a dry run that makes no changes')
@click.option('--collect-db', default=False, is_flag=True,
              help='Also save the database within the destination directory')
def _collect(db, destination, debug=False, dry_run=False, collect_db=False):
    config_logging(debug=debug)
    database = Database.from_file(db)
    database.collect_to_directory(destination)
    database.add_command(shlex.join(sys.argv))
    if not dry_run:
        database.to_file(db)
        if collect_db:
            database.to_file(Path(destination) / 'database' / Path(db).name)


@click.command('clean', help='Remove lower-priority alternatives of stored items')
@click.option('--db', type=click.Path(dir_okay=False), required=True,
              default='./photos.json', help='PhotoManager database path')
@click.option('--destination', type=click.Path(file_okay=False), required=True,
              help='Photo storage base directory')
@click.option('--subdir', type=str, default='',
              help='Remove only items within subdirectory')
@click.option('--debug', default=False, is_flag=True,
              help='Run in debug mode')
@click.option('--dry-run', default=False, is_flag=True,
              help='Perform a dry run that makes no changes')
def _clean(db, destination, subdir='', debug=False, dry_run=False):
    config_logging(debug=debug)
    database = Database.from_file(db)
    database.clean_stored_photos(destination, subdirectory=subdir, dry_run=dry_run)
    database.add_command(shlex.join(sys.argv))
    if not dry_run:
        database.to_file(db)


@click.command('verify', help='Verify checksums of stored items')
@click.option('--db', type=click.Path(dir_okay=False), required=True,
              default='./photos.json', help='PhotoManager database path')
@click.option('--destination', type=click.Path(file_okay=False), required=True,
              help='Photo storage base directory')
@click.option('--subdir', type=str, default='',
              help='Verify only items within subdirectory')
@click.option('--storage-type', type=str, default='HDD',
              help='Class of storage medium (HDD, SSD, RAID)')
def _verify(db, destination, subdir='', storage_type='HDD'):
    database = Database.from_file(db)
    num_errors = database.verify_stored_photos(destination, subdirectory=subdir, storage_type=storage_type)
    if num_errors:
        sys.exit(1)


@click.command('stats', help='Get database statistics')
@click.option('--db', type=click.Path(dir_okay=False), required=True,
              default='./photos.json', help='PhotoManager database path')
def _stats(db):
    database = Database.from_file(db)
    database.get_stats()


@click.group()
def main():
    pass


main.add_command(_create)
main.add_command(_import)
main.add_command(_collect)
main.add_command(_clean)
main.add_command(_verify)
main.add_command(_stats)


if __name__ == "__main__":
    main()
