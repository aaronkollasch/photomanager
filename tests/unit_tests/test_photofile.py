import pytest
from datetime import datetime, timezone, timedelta
from photomanager.database import PhotoFile


@pytest.mark.parametrize("offset", [-14400, -3600.0, 0, 10, 36000])
def test_tz_offset_local_datetime(offset):
    """
    PhotoFile.local_datetime returns a datetime for UTC timestamp with tz_offset
    """
    pf = PhotoFile(
        chk="d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
        src="A/img1.jpg",
        dt="N/A",
        ts=1438468116.9,
        fsz=771,
        tzo=offset,
    )
    print(repr(pf.local_datetime))
    assert (
        pf.local_datetime
        == datetime(
            *(2015, 8, 1, 22, 28, 36, 900000),
            tzinfo=timezone.utc,
        ).astimezone(timezone(timedelta(seconds=offset)))
    )


def test_local_time_string_none():
    """
    If no timezone is specified, PhotoFile uses the local time zone
    as an offset to timestamp.
    """
    pf = PhotoFile(
        chk="d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
        src="A/img1.jpg",
        dt="N/A",
        ts=1438468116.9,
        fsz=771,
    )
    print(repr(pf.local_datetime))
    local_tzinfo = datetime.now().astimezone().tzinfo
    assert (
        pf.local_datetime
        == datetime(
            *(2015, 8, 1, 22, 28, 36, 900000),
            tzinfo=timezone.utc,
        ).astimezone(local_tzinfo)
    )
