Changelog for PhotoManager
==========================

0.0.6 - 2021-11-24
------------------

Added
^^^^^
- BLAKE3 support (`55f749c <https://github.com/aaronkollasch/photomanager/commit/55f749c422b2e5e4b740146d332ea0269a6c481a>`_)
- Benchmarking scripts (`3d5261f <https://github.com/aaronkollasch/photomanager/commit/3d5261fa716089c41ab539832226f9f1602694c2>`_)
- Check for the existence of database file when parsing arguments (`32d7aa4 <https://github.com/aaronkollasch/photomanager/commit/32d7aa436c81ac45e9b9b606f258a4711585250f>`_)

0.0.5 - 2021-10-17
------------------

Added
^^^^^
- Fixed total file size in verify command (#16)

0.0.4 - 2021-10-17
------------------

Added
^^^^^
- Moved I/O operations from the Database class into a new subpackage. (#13)
- Added an option to verify a random fraction of photos (#14)

Fixed
^^^^^
- Fixed log message display for the verify command and improved formatting (#15)

0.0.3 - 2021-06-13
------------------

Reverted
^^^^^^^^

- Use str internally to represent checksum
  (`692f7ec <https://github.com/aaronkollasch/photomanager/commit/692f7ec49ff9e7753f3dc48e27529baa2b1fe3be>`_)

0.0.2 - 2021-06-13
------------------

Added
^^^^^

- A configurable timezone offset for each PhotoFile
  and for each import, #11
- Unified interface for AsyncFileHasher and AsyncPyExifTool
  (`2e0cd82 <https://github.com/aaronkollasch/photomanager/commit/2e0cd82de13be5399436952c2fd9de17c3d05c69>`_)
- Reduced database file size, #11
- Sped up pyexiftool
  (`4f6e4ca <https://github.com/aaronkollasch/photomanager/commit/4f6e4cae5115a02efb16d889e9901a0bcc816d34>`_)
- Sped up database loading and unloading
  (`499c944 <https://github.com/aaronkollasch/photomanager/commit/499c944c8c6232653b7ecce73a11e83113add84e>`_)

Fixed
^^^^^

- Added check in Database.map_hashes() for old unmapped hashes
  (`71b3b79 <https://github.com/aaronkollasch/photomanager/commit/71b3b7935c63187cf56dc12fc2f145de539f6ee5>`_)

0.0.1 - 2021-06-02
------------------

- [NEW] Initial package creation
