from io import BytesIO
import pytest
from photomanager.hasher_async import file_checksum

checksum_expected_results = [
    {
        "algorithm": "blake2b-256",
        "bytes": b"",
        "checksum": "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8",
    },
    {
        "algorithm": "blake2b-256",
        "bytes": b"\xde\xad\xbe\xef",
        "checksum": "f3e925002fed7cc0ded46842569eb5c90c910c091d8d04a1bdf96e0db719fd91",
    },
    {
        "algorithm": "sha256",
        "bytes": b"",
        "checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    },
    {
        "algorithm": "sha256",
        "bytes": b"\xde\xad\xbe\xef",
        "checksum": "5f78c33274e43fa9de5659265c1d917e25c03722dcb0b8d27db8d5feaa813953",
    },
]


@pytest.mark.parametrize("checksum", checksum_expected_results)
def test_file_checksum(checksum):
    with BytesIO(checksum["bytes"]) as f:
        assert file_checksum(f, algorithm=checksum["algorithm"]) == checksum["checksum"]
