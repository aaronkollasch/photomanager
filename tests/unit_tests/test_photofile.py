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
    assert pf.local_datetime == datetime(
        *(2015, 8, 1, 22, 28, 36, 900000),
        tzinfo=timezone.utc,
    ).astimezone(timezone(timedelta(seconds=offset)))


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
    assert pf.local_datetime == datetime(
        *(2015, 8, 1, 22, 28, 36, 900000),
        tzinfo=timezone.utc,
    ).astimezone(local_tzinfo)


class TestPhotoFile:
    def test_to_dict(self):
        """
        PhotoFile.to_dict() returns a dictionary with the PhotoFile's attributes.
        """
        assert PhotoFile(
            chk="deadbeef",
            src="/a/b/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="/d/e/f.jpg",
            prio=11,
            tzo=-14400,
        ).to_dict() == {
            "chk": "deadbeef",
            "src": "/a/b/c.jpg",
            "dt": "2015:08:27 04:09:36.50",
            "ts": 1440662976.5,
            "fsz": 1024,
            "sto": "/d/e/f.jpg",
            "prio": 11,
            "tzo": -14400,
        }

    def test_to_json(self):
        """
        PhotoFile.__dict__ is a dictionary with the PhotoFile's attributes.
        The __dict__ property is used by orjson for json conversion.
        """
        pf = PhotoFile(
            chk="deadbeef",
            src="/a/b/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="/d/e/f.jpg",
            prio=11,
            tzo=-14400,
        )
        print(pf)
        print(pf.__getattribute__("__dict__"))
        assert pf.__dict__ == {
            "chk": "deadbeef",
            "src": "/a/b/c.jpg",
            "dt": "2015:08:27 04:09:36.50",
            "ts": 1440662976.5,
            "fsz": 1024,
            "sto": "/d/e/f.jpg",
            "prio": 11,
            "tzo": -14400,
        }

    def test_eq(self):
        """
        PhotoFiles with the same attributes are equal.
        """
        assert PhotoFile(
            chk="deadbeef",
            src="/a/b/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="/d/e/f.jpg",
            prio=11,
        ) == PhotoFile(
            chk="deadbeef",
            src="/a/b/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="/d/e/f.jpg",
            prio=11,
        )

    def test_neq(self):
        """
        PhotoFiles with different attributes are not equal.
        """
        pf1 = PhotoFile(
            chk="deadbeef",
            src="/a/b/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="/d/e/f.jpg",
            prio=11,
        )
        assert pf1 != PhotoFile(
            chk="deadfeed",
            src="/a/b/c.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="/d/e/f.jpg",
            prio=11,
        )
        assert pf1 != PhotoFile(
            chk="deadbeef",
            src="/a/b/d.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.5,
            fsz=1024,
            sto="/d/e/f.jpg",
            prio=11,
        )
        assert pf1 != PhotoFile(
            chk="deadbeef",
            src="/a/b/d.jpg",
            dt="2015:08:27 04:09:36.50",
            ts=1440662976.0,
            fsz=1024,
            sto="/d/e/f.jpg",
            prio=11,
        )
