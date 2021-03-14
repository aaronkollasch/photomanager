#!/usr/bin/env python
import sys
from pathlib import Path
import re
import logging
from datetime import datetime
import click
from pyexiftool import ExifTool
from database import Database


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


@click.command('import', help='Find and add items to database')
@click.option('--db', type=click.Path(dir_okay=False), required=True,
              default='./photos.json', help='PhotoManager database path')
@click.option('--source', type=click.Path(file_okay=False),
              help='Directory to import')
@click.option('--file', type=click.Path(dir_okay=False),
              help='File to import')
@click.option('--exclude', multiple=True,
              help='Name patterns to exclude')
@click.option('--priority', type=int, default=10,
              help='Priority of imported photos (lower is preferred)')
@click.option('--debug', default=False, is_flag=True,
              help='Run in debug mode')
@click.argument('paths', nargs=-1, type=click.Path())
def _import(db, source, file, exclude, paths, debug=False, priority=10):
    if not source and not file and not paths:
        print("Nothing to import")
        sys.exit(1)
    if debug:
        logging.basicConfig(level=logging.DEBUG)
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

    database.import_photos(files=filtered_files, priority=priority)
    database.db['command_history'][datetime.now().strftime('%Y-%m-%d_%H-%M-%S')] = ' '.join(sys.argv)
    database.to_file(db)


@click.command('collect', help='Collect highest-priority items into storage')
@click.option('--db', type=click.Path(dir_okay=False), required=True,
              default='./photos.json', help='PhotoManager database path')
@click.option('--destination', type=click.Path(file_okay=False), required=True,
              help='Photo storage base directory')
@click.option('--debug', default=False, is_flag=True,
              help='Run in debug mode')
def _collect(db, destination, debug=False):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    database = Database.from_file(db)
    database.collect_to_directory(destination)
    database.db['command_history'][datetime.now().strftime('%Y-%m-%d_%H-%M-%S')] = ' '.join(sys.argv)
    database.to_file(db)


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
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    database = Database.from_file(db)
    database.clean_stored_photos(destination, subdirectory=subdir, dry_run=dry_run)
    database.db['command_history'][datetime.now().strftime('%Y-%m-%d_%H-%M-%S')] = ' '.join(sys.argv)
    if not dry_run:
        database.to_file(db)


@click.command('verify', help='Verify checksums of stored items')
@click.option('--db', type=click.Path(dir_okay=False), required=True,
              default='./photos.json', help='PhotoManager database path')
@click.option('--destination', type=click.Path(file_okay=False), required=True,
              help='Photo storage base directory')
@click.option('--subdir', type=str, default='',
              help='Verify only items within subdirectory')
def _verify(db, destination, subdir=''):
    database = Database.from_file(db)
    database.verify_stored_photos(destination, subdirectory=subdir)


@click.command('stats', help='Get database statistics')
@click.option('--db', type=click.Path(dir_okay=False), required=True,
              default='./photos.json', help='PhotoManager database path')
def _stats(db):
    database = Database.from_file(db)
    database.get_stats()

@click.group()
def main():
    pass


main.add_command(_import)
main.add_command(_collect)
main.add_command(_clean)
main.add_command(_verify)
main.add_command(_stats)


if __name__ == "__main__":
    with ExifTool() as et:
        main()
