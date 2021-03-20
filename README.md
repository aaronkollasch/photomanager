# PhotoManager

A manager for photos and other media files
inspired by [elodie](https://github.com/jmathai/elodie).

Imports photos into a database and collects them to a specified directory.
Verifies stored photos based on their checksum.
Database is stored in a non-proprietary JSON format.
Will not modify any photos.  

Photos are organized by the best available date
obtained from EXIF or non-EXIF information.
They can be prioritized so that only the best available version
will be collected. Alternate and derived versions of photos
are identified by matching filenames and timestamps.


## Usage
```
Usage: photomanager.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  clean    Remove lower-priority alternatives of stored items
  collect  Collect highest-priority items into storage
  import   Find and add items to database
  stats    Get database statistics
  verify   Verify checksums of stored items
```

### Import photos
```
Usage: photomanager.py import [OPTIONS] [PATHS]...

  Find and add items to database

Options:
  --db FILE            PhotoManager database path  [required]
  --source DIRECTORY   Directory to import
  --file FILE          File to import
  --exclude TEXT       Name patterns to exclude
  --priority INTEGER   Priority of imported photos (lower is preferred,
                       default=10)

  --storage-type TEXT  Class of storage medium (HDD or SSD)
  --debug              Run in debug mode
  --help               Show this message and exit.
```

### Collect photos
```
Usage: photomanager.py collect [OPTIONS]

  Collect highest-priority items into storage

Options:
  --db FILE                PhotoManager database path  [required]
  --destination DIRECTORY  Photo storage base directory  [required]
  --debug                  Run in debug mode
  --help                   Show this message and exit.
```

### Verify photos
```
Usage: photomanager.py verify [OPTIONS]

  Verify checksums of stored items

Options:
  --db FILE                PhotoManager database path  [required]
  --destination DIRECTORY  Photo storage base directory  [required]
  --subdir TEXT            Verify only items within subdirectory
  --help                   Show this message and exit.
```

### Remove unnecessary duplicates
```
Usage: photomanager.py clean [OPTIONS]

  Remove lower-priority alternatives of stored items

Options:
  --db FILE                PhotoManager database path  [required]
  --destination DIRECTORY  Photo storage base directory  [required]
  --subdir TEXT            Remove only items within subdirectory
  --debug                  Run in debug mode
  --dry-run                Perform a dry run that makes no changes
  --help                   Show this message and exit.
```

## Requirements
- Python 3.9
- click  
- tqdm
- [ExifTool](https://exiftool.org/)
