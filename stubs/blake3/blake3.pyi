from hashlib import _Hash
from typing import ClassVar

class blake3(_Hash):
    AUTO: ClassVar[int] = ...
    block_size: ClassVar[int] = ...
    digest_size: ClassVar[int] = ...
    key_size: ClassVar[int] = ...
    name: ClassVar[str] = ...
    @classmethod
    def __init__(cls, *args, **kwargs) -> None: ...
