============
PhotoManager
============

.. image:: https://github.com/aaronkollasch/photomanager/workflows/CI/badge.svg?branch=main
     :target: https://github.com/aaronkollasch/photomanager/actions?workflow=CI
     :alt: CI Status

.. image:: https://codecov.io/gh/aaronkollasch/photomanager/branch/main/graph/badge.svg?token=QLC34GSAMR
     :target: https://codecov.io/gh/aaronkollasch/photomanager
     :alt: Test Coverage

A manager for photos and other media files,
inspired by `elodie <https://github.com/jmathai/elodie>`_.

Indexes photos, adds them to a database, and 
collects them in a specified directory.
Verifies stored photos based on their checksum.
Database is stored in a non-proprietary, human-readable JSON format.
Will not modify any photos.

Photos are organized by the best available date
obtained from metadata or file information.
They can be prioritized so that only the best available version
will be collected. Alternate and derived versions of photos
are identified by matching filenames and timestamps.

Installation
============

Requires Python 3.8 or 3.9.

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

Usage
=====

Add photos to the database
--------------------------

.. code-block:: bash

    photomanager index --debug --db db.json /path/to/directory /path/to/photo.jpg

PhotoManager will search for media files in any supplied directories
and also index single files supplied directly as arguments.
Repeat with as many sources as desired.

For lower-quality versions of source photos such as downstream edits
or previews, provide a lower priority such as ``--priority 30``
(default is 10). These will be collected if the original (high-priority)
copy is unavailable. Alternate versions are matched using their
timestamp and filename.

Old versions of the database are given unique names and not overwritten.

The database takes this form:

.. code-block:: json

    {
      "version": 1,
      "hash_algorithm": "blake2b-256",
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
      "checksum": "881f279108bcec5b6e...",
      "source_path": "/path/to/photo_123.jpg",
      "datetime": "2021:03:29 06:40:00+00:00",
      "timestamp": 1617000000,
      "file_size": 123456,
      "store_path": "2021/03-Mar/2021-03-29_02-40-00-881f279-photo_123.jpg",
      "priority": 10
    }

If the photos are stored on an SSD or RAID array, use
``--storage-type SSD`` or ``--storage-type RAID`` and
checksum and EXIF checks will be performed by multiple workers.

Collect files into a storage folder
-----------------------------------

Now that PhotoManager knows what photos you want to store,
collect them into a storage folder:

.. code-block:: bash

    photomanager collect --debug --db db.json --destination /path/to/destination

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

Stored photo paths in the database are relative to ``destination``,
so the library is portable, and the same database can be shared across
library copies. Recommended syncing tools are ``rsync`` and ``rclone``.

Indexing and collection can be repeated
as new sources of photos are found and collected.

Verify stored photos against bit rot or modification
----------------------------------------------------

.. code-block:: bash

    photomanager verify --db db.json --destination /path/to/destination

If the photos are stored on an SSD or RAID array,
use ``--storage-type SSD`` or ``--storage-type RAID`` and
multiple files will be verified in parallel.

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
`This command is only needed if you want to use a
non-default hashing algorithm.`

::

    Usage: photomanager create [OPTIONS]

      Create an empty database

    Options:
      --db FILE              PhotoManager database path (.json). Add
                             extensions .zst or .gz to compress.  [required]
      --hash-algorithm TEXT  Hash algorithm (default=blake2b-256)
      --help                 Show this message and exit.

Index photos
------------

::

    Usage: photomanager index [OPTIONS] [PATHS]...

      Find and add items to database

    Options:
      --db FILE            PhotoManager database filepath (.json). Add extensions
                           .zst or .gz to compress.  [required]
      --source DIRECTORY   Directory to index
      --file FILE          File to index
      --exclude TEXT       Name patterns to exclude
      --priority INTEGER   Priority of indexed photos (lower is preferred,
                           default=10)
      --storage-type TEXT  Class of storage medium (HDD, SSD, RAID)
      --debug              Run in debug mode
      --help               Show this message and exit.

Collect photos
--------------

::

    Usage: photomanager collect [OPTIONS]

      Collect highest-priority items into storage

    Options:
      --db FILE                PhotoManager database path  [required]
      --destination DIRECTORY  Photo storage base directory  [required]
      --debug                  Run in debug mode
      --collect-db             Also save the database within destination
      --help                   Show this message and exit.

Verify photos
-------------

::

    Usage: photomanager verify [OPTIONS]

      Verify checksums of stored items

    Options:
      --db FILE                PhotoManager database path  [required]
      --destination DIRECTORY  Photo storage base directory  [required]
      --subdir TEXT            Verify only items within subdirectory
      --storage-type TEXT      Class of storage medium (HDD, SSD, RAID)
      --help                   Show this message and exit.

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
      --help                   Show this message and exit.
