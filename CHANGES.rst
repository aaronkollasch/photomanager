Changelog for PhotoManager
==========================

Unreleased - 2021-06-13
-----------------------

Reverted
^^^^^^^^

- Use str internally to represent checksum

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
