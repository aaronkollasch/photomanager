from ._version import version

__version__ = version


class PhotoManagerBaseException(Exception):
    pass


class PhotoManagerException(PhotoManagerBaseException):
    pass
