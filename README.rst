============
PhotoManager
============

.. image:: https://github.com/aaronkollasch/photomanager/workflows/CI/badge.svg?branch=main
     :target: https://github.com/aaronkollasch/photomanager/actions?workflow=CI
     :alt: CI Status

.. image:: http://www.mypy-lang.org/static/mypy_badge.svg
     :target: http://mypy-lang.org/
     :alt: Checked with mypy

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
     :target: https://github.com/psf/black
     :alt: Code style: black

.. image:: https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336
     :target: https://pycqa.github.io/isort/
     :alt: Imports: isort

A manager for photos and other media files.

Indexes photos, adds them to a database, and
collects them in a specified directory.
Verifies stored photos against bitrot or modification
based on their checksum.
Database is stored in a non-proprietary, human-readable JSON format.
PhotoManager is inspired by `elodie <https://github.com/jmathai/elodie>`_,
but it is intended for archiving and will not modify any file contents,
including metadata.

Photos are organized by the best available date
obtained from metadata or file information.
They can be prioritized so that only the best available version
will be collected. Alternate copies of photos
are identified by matching filenames and timestamps.

Installation
============

Requires Python 3.10, 3.11, or 3.12.

Clone the repository
--------------------

.. code-block:: bash

    git clone https://github.com/aaronkollasch/photomanager.git
    cd photomanager
    pip install .


Install ExifTool
----------------

ExifTool is required to index,
but not to collect or verify photos.

.. code-block:: bash

    # macOS
    brew install exiftool

    # Debian / Ubuntu
    apt install libimage-exiftool-perl

    # Fedora / Redhat
    dnf install perl-Image-ExifTool

Or download from `<https://exiftool.org>`_

Extras
------

The database JSON file can optionally be compressed as a zstd
or gzip file. Zstandard is available in most package managers,
e.g. ``brew install zstd``.
Filenames ending in ``.gz`` will be read as gzip archives and
names ending in ``.zst`` will be read as zstd archives.

To enable photo integrity checking, additional dependencies
must be installed with ``pip install .[check-mi]``.

Usage
=====

Add photos to the database
--------------------------

.. code-block:: bash

    photomanager index --db db.json /path/to/directory /path/to/photo.jpg

PhotoManager will search for media files in any supplied directories
and also index single files supplied directly as arguments.
Repeat with as many sources as desired.

For lower-quality versions of source photos such as downstream edits
or previews, provide a lower priority such as ``--priority 30``
(default is 10). These will be collected if the original (high-priority)
copy is unavailable. Alternate versions are matched using their
timestamp and filename.

Previous versions of the database are given unique names and not overwritten.

If the photos are stored on an SSD or RAID array, use
``--storage-type SSD`` or ``--storage-type RAID`` and
checksum and EXIF checks will be performed by multiple workers.

To check the integrity of media files before indexing them,
use the ``--check-integrity`` flag.
Integrity checking has additional dependencies; install them with
``pip install .[check-mi]``

Collect files into a storage folder
-----------------------------------

Now that PhotoManager knows what photos you want to store,
collect them into a storage folder:

.. code-block:: bash

    photomanager collect --db db.json --destination /path/to/destination

This will copy the highest-priority versions of photos
not already stored into the destination folder and
give them consistent paths based on their
timestamps, checksums, and original names.

::

    ├── 2015
    │   ├── 01-Jan
    │   │   ├── 2015-01-04_10-22-03-a927bc3-IMG_0392.JPG
    │   │   └── 2015-01-31_19-20-13-ce028af-IMG_0782.JPG
    │   └── 02-Feb
    │       └── 2015-02-30_02-40-43-9637179-AWK_0060.jpg
    ├── 2016
    │   ├── 05-May
    │   │   ├── 2018-05-24_00-31-08-bf3ed29-IMG_8213.JPG
    │   │   └── 2018-05-29_20-13-16-39a4187-IMG_8591.MOV
    ├── 2017
    │   ├── 12-Dec
    │   │   ├── 2017-12-25_20-32-41-589c151-DSC_8705.JPG
    │   │   └── 2017-12-25_20-32-41-4bb6987-DSC_8705.NEF

Stored photo paths in the database are relative to the ``destination`` folder,
so the library is portable, and the same database can be shared across
library copies. Recommended syncing tools are ``rsync`` and ``rclone``.

Indexing and collection can be repeated
as new sources of photos are found and collected.
The ``import`` command performs both these actions in a single command:

.. code-block:: bash

    photomanager import --db db.json --destination /path/to/destination /path/to/source/directory

Verify stored photos against bit rot or modification
----------------------------------------------------

.. code-block:: bash

    photomanager verify --db db.json --destination /path/to/destination

If the photos are stored on an SSD or RAID array,
use ``--storage-type SSD`` or ``--storage-type RAID`` and
multiple files will be verified in parallel.

Note that this can only detect unexpected modifications;
it cannot undo changes it detects.
Therefore, backing up the storage directory to multiple locations
(such as with a `3-2-1 backup <https://github.com/geerlingguy/my-backup-plan>`_) is recommended.

Usage instructions
==================

Use the ``--help`` argument to see instructions for each command

::

    photomanager --help
    Usage: photomanager [OPTIONS] COMMAND [ARGS]...

    Options:
      --help  Show this message and exit.

    Commands:
      clean    Remove lower-priority alternatives of stored items
      collect  Collect highest-priority items into storage
      create   Create an empty database
      import   Index items and collect to directory
      index    Find and add items to database
      stats    Get database statistics
      verify   Verify checksums of stored items

Create database
---------------
`This command is only needed if you want to specify a
non-default hashing algorithm or timezone.`

Supported hashes are blake2b-256 (the default) and sha256.
These are equivalent to ``b2sum -l 256`` and ``sha256sum``, respectively.
BLAKE2b is recommended as it is faster (and stronger) than SHA-2,
resulting in noticeably faster indexing/verification on fast storage,
and less CPU usage on slow storage.

::

    Usage: photomanager create [OPTIONS]

      Create a database. Save a new version if it already exists.

    Options:
      --db FILE                       PhotoManager database filepath (.json). Add
                                      extensions .zst or .gz to compress.
                                      [required]
      --hash-algorithm [sha256|blake2b-256|blake3]
                                      Hash algorithm (default=blake2b-256)
      --timezone-default TEXT         Timezone to use when indexing timezone-naive
                                      photos (example="-0400", default="local")
      --debug                         Run in debug mode
      -h, --help                      Show this message and exit.

Index photos
------------

::

    Usage: photomanager index [OPTIONS] [PATHS]...

      Index and add items to database

    Options:
      --db FILE                       PhotoManager database filepath (.json). Add
                                      extensions .zst or .gz to compress.
      --source DIRECTORY              Directory to index
      --file FILE                     File to index
      --exclude TEXT                  Name patterns to exclude
      --skip-existing                 Don't index files that are already in the
                                      database
      --check-integrity               Check media integrity and don't index bad
                                      files
      --priority INTEGER              Priority of indexed photos (lower is
                                      preferred, default=10)
      --timezone-default TEXT         Timezone to use when indexing timezone-naive
                                      photos (example="-0400", default="local")
      --hash-algorithm [sha256|blake2b-256|blake3]
                                      Hash algorithm to use if no database
                                      provided (default=blake2b-256)
      --storage-type [HDD|SSD|RAID]   Class of storage medium (HDD, SSD, RAID)
      --debug                         Run in debug mode
      --dump                          Print photo info to stdout
      --dry-run                       Perform a dry run that makes no changes
      -h, --help                      Show this message and exit.

Collect photos
--------------

::

    Usage: photomanager collect [OPTIONS]

      Collect highest-priority items into storage

    Options:
      --db FILE                PhotoManager database path  [required]
      --destination DIRECTORY  Photo storage base directory  [required]
      --debug                  Run in debug mode
      --dry-run                Perform a dry run that makes no changes
      --collect-db             Also save the database within destination
      -h, --help               Show this message and exit.

Verify photos
-------------

::

    Usage: photomanager verify [OPTIONS]

      Verify checksums of stored items

    Options:
      --db FILE                      PhotoManager database path  [required]
      --destination DIRECTORY        Photo storage base directory  [required]
      --subdir TEXT                  Verify only items within subdirectory
      --storage-type [HDD|SSD|RAID]  Class of storage medium (HDD, SSD, RAID)
      --random-fraction FLOAT        Verify a randomly sampled fraction of the
                                     photos
      --debug                        Run in debug mode
      -h, --help                     Show this message and exit.

Remove unnecessary duplicates
-----------------------------

::

    Usage: photomanager clean [OPTIONS]

      Remove lower-priority alternatives of stored items

    Options:
      --db FILE                PhotoManager database path  [required]
      --destination DIRECTORY  Photo storage base directory  [required]
      --subdir TEXT            Remove only items within subdirectory
      --debug                  Run in debug mode
      --dry-run                Perform a dry run that makes no changes
      -h, --help               Show this message and exit.

Database file format
====================

The database is a json file, optionally gzip or zstd-compressed.
It takes this form:

.. code-block:: json

    {
      "version": 3,
      "hash_algorithm": "blake2b-256",
      "timezone_default": "local",
      "photo_db": {
        "<uid>": [
          "<photo>",
          "<photo>",
          "..."
        ]
      },
      "command_history": {
        "<timestamp>": "<command>"
      }
    }

where an example photo has the form:

.. code-block:: json

    {
      "chk": "881f279108bcec5b6e...",
      "src": "/path/to/photo_123.jpg",
      "dt": "2021:03:29 06:40:00+00:00",
      "ts": 1617000000,
      "fsz": 123456,
      "sto": "2021/03-Mar/2021-03-29_02-40-00-881f279-photo_123.jpg",
      "prio": 10,
      "tzo": -14400.0
    }

Attributes:

:chk (str):   Checksum of photo file
:src (str):   Absolute path where photo was found
:dt (str):    Datetime string for best estimated creation date (original)
:ts (float):  POSIX timestamp of best estimated creation date (derived)
:fsz (int):   Photo file size, in bytes
:sto (str):   Relative path where photo is stored, empty if not stored
:prio (int):  Photo priority (lower is preferred)
:tzo (float): Local time zone offset (optional)

