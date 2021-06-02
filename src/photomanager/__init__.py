from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
version = __version__


class PhotoManagerBaseException(Exception):
    pass


class PhotoManagerException(PhotoManagerBaseException):
    pass
