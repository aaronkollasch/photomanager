import datetime

import pytest

from photomanager.database import Database
from photomanager.photofile import datetime_str_to_object
from photomanager.pyexiftool import best_datetime

best_datetime_expected_results = [
    {
        "metadata": {
            "SourceFile": "/images/img1.jpg",
            "Composite:SubSecDateTimeOriginal": "2015:08:01 18:28:36.90",
            "EXIF:DateTimeOriginal": "2015:08:01 18:28:36",
            "EXIF:CreateDate": "2015:08:01 18:28:36",
            "XMP:CreateDate": "2015:08:01 18:28:36.90",
            "EXIF:SubSecTimeOriginal": 90,
            "File:FileCreateDate": "2015:08:03 17:54:40-04:00",
            "File:FileModifyDate": "2015:08:03 17:54:42-04:00",
        },
        "best_datetime": "2015:08:01 18:28:36.90",
    },
    {
        "metadata": {
            "SourceFile": "/images/img2.MOV",
            "QuickTime:CreationDate": "2019:12:27 20:56:06-06:00",
            "QuickTime:CreateDate": "2019:12:28 02:56:06",
            "File:FileCreateDate": "2019:12:27 20:56:06-05:00",
            "File:FileModifyDate": "2019:12:27 20:56:06-05:00",
        },
        "best_datetime": "2019:12:27 20:56:06-06:00",
    },
    {
        "metadata": {
            "SourceFile": "/images/img3.HEIC",
            "Composite:SubSecDateTimeOriginal": "2021:02:08 21:45:01.233-06:00",
            "EXIF:DateTimeOriginal": "2021:02:08 21:45:01",
            "EXIF:CreateDate": "2021:02:08 21:45:01",
            "XMP:CreateDate": "2021:02:08 21:45:01",
            "EXIF:SubSecTimeOriginal": 233,
            "EXIF:OffsetTimeOriginal": "-06:00",
            "File:FileCreateDate": "2021:02:08 23:19:05-05:00",
            "File:FileModifyDate": "2021:02:08 23:19:05-05:00",
        },
        "best_datetime": "2021:02:08 21:45:01.233-06:00",
    },
    {
        "metadata": {
            "SourceFile": "/images/img4.HEIC",
            "EXIF:DateTimeOriginal": "2021:02:08 21:45:01",
            "EXIF:CreateDate": "2021:02:08 21:45:01",
            "XMP:CreateDate": "2021:02:08 21:45:01",
            "EXIF:SubSecTimeOriginal": 233,
            "EXIF:OffsetTimeOriginal": "-06:00",
            "File:FileCreateDate": "2021:02:08 23:19:05-05:00",
            "File:FileModifyDate": "2021:02:08 23:19:05-05:00",
        },
        "best_datetime": "2021:02:08 21:45:01.233-06:00",
    },
    {
        "metadata": {
            "SourceFile": "/images/img5.HEIC",
            "EXIF:DateTimeOriginal": "2021:02:08 21:45:01",
            "EXIF:CreateDate": "2021:02:08 21:45:01",
            "XMP:CreateDate": "2021:02:08 21:45:01",
            "File:FileCreateDate": "2021:02:08 23:19:05-05:00",
            "File:FileModifyDate": "2021:02:08 23:19:05-05:00",
        },
        "best_datetime": "2021:02:08 21:45:01",
    },
    {
        "metadata": {
            "SourceFile": "/images/img6.HEIC",
            "EXIF:CreateDate": "2021:02:08 21:45:01",
            "XMP:CreateDate": "2021:02:08 21:45:01",
            "File:FileCreateDate": "2021:02:08 23:19:05-05:00",
            "File:FileModifyDate": "2021:02:08 23:19:05-05:00",
        },
        "best_datetime": "2021:02:08 21:45:01",
    },
    {
        "metadata": {
            "SourceFile": "/images/img7.HEIC",
            "XMP:CreateDate": "2021:02:08 21:45:01",
            "File:FileCreateDate": "2021:02:08 23:19:05-05:00",
            "File:FileModifyDate": "2021:02:08 23:19:05-05:00",
        },
        "best_datetime": "2021:02:08 21:45:01",
    },
    {
        "metadata": {
            "SourceFile": "/images/img7.HEIC",
            "XMP:DateTimeOriginal": "2021:02:08 21:45:02",
            "XMP:CreateDate": "2021:02:08 21:45:01",
            "File:FileCreateDate": "2021:02:08 23:19:05-05:00",
            "File:FileModifyDate": "2021:02:08 23:19:05-05:00",
        },
        "best_datetime": "2021:02:08 21:45:02",
    },
    {
        "metadata": {
            "SourceFile": "/images/img7.HEIC",
            "EXIF:CreationDate": "2021:02:08 21:45:02",
            "XMP:CreateDate": "2021:02:08 21:45:01",
            "File:FileCreateDate": "2021:02:08 23:19:05-05:00",
            "File:FileModifyDate": "2021:02:08 23:19:05-05:00",
        },
        "best_datetime": "2021:02:08 21:45:02",
    },
    {
        "metadata": {
            "SourceFile": "/images/img8.MP4",
            "QuickTime:CreateDate": "2020:05:20 17:39:39",
            "File:FileCreateDate": "2020:05:20 12:39:39-04:00",
            "File:FileModifyDate": "2020:05:20 12:39:39-04:00",
        },
        "best_datetime": "2020:05:20 17:39:39",
    },
    {
        "metadata": {
            "SourceFile": "/images/img8.MP4",
            "QuickTime:CreateDate": "0000:05:20 12:39:39",
            "File:FileCreateDate": "2020:05:20 12:39:39-04:00",
            "File:FileModifyDate": "2020:05:20 12:39:39-04:00",
        },
        "best_datetime": "2020:05:20 12:39:39-04:00",
    },
    {
        "metadata": {
            "SourceFile": "/images/img8.MP4",
            "QuickTime:CreateDate": "0000:00:00 00:00:00",
            "File:FileCreateDate": "0000:05:20 12:39:39",
            "File:FileModifyDate": "2020:05:20 12:39:39-04:00",
        },
        "best_datetime": "2020:05:20 12:39:39-04:00",
    },
    {
        "metadata": {
            "SourceFile": "/images/img8.MP4",
            "File:FileCreateDate": "2020:05:20 12:39:39-04:00",
            "File:FileModifyDate": "2020:05:20 12:39:39-04:00",
        },
        "best_datetime": "2020:05:20 12:39:39-04:00",
    },
    {
        "metadata": {
            "SourceFile": "/images/img9.JPG",
            "Composite:SubSecDateTimeOriginal": "2015:08:27 04:09:36.50",
            "EXIF:DateTimeOriginal": "2015:08:27 04:09:36",
            "EXIF:CreateDate": "2015:08:27 04:09:36",
            "EXIF:SubSecTimeOriginal": 50,
            "File:FileCreateDate": "2015:08:27 05:09:36-04:00",
            "File:FileModifyDate": "2015:08:27 05:09:36-04:00",
        },
        "best_datetime": "2015:08:27 04:09:36.50",
    },
]


@pytest.mark.parametrize("datetime_test", best_datetime_expected_results)
def test_best_datetime(datetime_test):
    assert best_datetime(datetime_test["metadata"]) == datetime_test["best_datetime"]


DEFAULT_TZ = datetime.timezone(datetime.timedelta(days=-1, seconds=72000))
datetime_ts_expected_results = [
    {
        "timestamp_str": "2015:08:27 04:09:36.50",
        "datetime_obj": datetime.datetime(
            2015, 8, 27, 4, 9, 36, 500000, tzinfo=DEFAULT_TZ
        ),
        "timestamp": 1440662976.5,
    },
    {
        "timestamp_str": "2015:08:27 04:09:36",
        "datetime_obj": datetime.datetime(2015, 8, 27, 4, 9, 36, tzinfo=DEFAULT_TZ),
        "timestamp": 1440662976.0,
    },
    {
        "timestamp_str": "2015:08:27 04:09",
        "datetime_obj": datetime.datetime(2015, 8, 27, 4, 9, tzinfo=DEFAULT_TZ),
        "timestamp": 1440662940.0,
    },
    {
        "timestamp_str": "2015:08:27 04:09:36.50Z",
        "datetime_obj": datetime.datetime(
            2015, 8, 27, 4, 9, 36, 500000, tzinfo=datetime.timezone.utc
        ),
        "timestamp": 1440648576.5,
    },
    {
        "timestamp_str": "2015:08:27 04:09:36.50-0400",
        "datetime_obj": datetime.datetime(
            *(2015, 8, 27, 4, 9, 36, 500000),
            tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=72000)),
        ),
        "timestamp": 1440662976.5,
    },
    {
        "timestamp_str": "2015:08:27 04:09-0300",
        "datetime_obj": datetime.datetime(
            *(2015, 8, 27, 4, 9),
            tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=75600)),
        ),
        "timestamp": 1440659340.0,
    },
]


@pytest.mark.parametrize("datetime_test", datetime_ts_expected_results)
def test_datetime_to_timestamp(datetime_test):
    datetime_obj = datetime_str_to_object(
        datetime_test["timestamp_str"],
        tz_default=DEFAULT_TZ,
    )
    assert datetime_obj == datetime_test["datetime_obj"]
    assert datetime_obj.timestamp() == datetime_test["timestamp"]


def test_datetime_to_timestamp_errors():
    with pytest.raises(ValueError):
        datetime_str_to_object("2015:08:2704:09-0400")
    with pytest.raises(ValueError):
        datetime_str_to_object("2015:08:27 04:09a")
    with pytest.raises(ValueError):
        datetime_str_to_object("2015:08:27 04:09.a")


expected_tzinfo = [
    ("local", None, False),
    ("-0400", datetime.timezone(datetime.timedelta(days=-1, seconds=72000)), False),
    ("Z", datetime.timezone.utc, False),
    ("+1000", datetime.timezone(datetime.timedelta(seconds=36000)), False),
    ("wrong", None, True),
]


@pytest.mark.parametrize("tz_str,tz_info,error", expected_tzinfo)
def test_database_timezone_default(tz_str, tz_info, error, caplog):
    db = Database()
    db._db["timezone_default"] = tz_str
    assert db.timezone_default == tz_info
    if error:
        assert caplog.messages
    else:
        assert not caplog.messages
