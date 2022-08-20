import asyncio
import logging

import orjson
import pytest

from photomanager.pyexiftool import AsyncExifTool, ExifTool, ExifToolJob

from . import AsyncNopProcess, NopProcess


class TestAsyncPyExifTool:
    def test_get_metadata_batch(self, monkeypatch, caplog):
        """
        AsyncExifTool get_metadata_batch processes json output by exiftool
        into a dict from filename to a metadata dict.
        """

        async def nop_cse(*_, **__):
            loop = asyncio.events.get_event_loop()
            loop.set_debug(True)
            return AsyncNopProcess(
                stdout_messages=(
                    b'[{"SourceFile":"img1.jpg","EXIF:DateTimeOriginal":'
                    b'"2015:08:27 04:09:36"}]\n',
                    b"{ready}",
                )
            )

        monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
        caplog.set_level(logging.DEBUG)
        metadata = AsyncExifTool(batch_size=10).get_metadata_batch(["img1.jpg"])
        print([(r.levelname, r) for r in caplog.records])
        print(metadata)
        assert not any(record.levelname == "WARNING" for record in caplog.records)
        assert len(metadata) == 1
        assert metadata["img1.jpg"]["EXIF:DateTimeOriginal"] == "2015:08:27 04:09:36"

    def test_empty_response_warning(self, monkeypatch, caplog):
        """
        AsyncExifTool logs a warning if exiftool returns an empty response
        """

        async def nop_cse(*_, **__):
            loop = asyncio.events.get_event_loop()
            loop.set_debug(True)
            return AsyncNopProcess(
                stdout_messages=(
                    b"\n",
                    b"{ready}",
                )
            )

        monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
        caplog.set_level(logging.DEBUG)
        metadata = AsyncExifTool(batch_size=10).get_metadata_batch(["asdf.bin"])
        print([(r.levelname, r) for r in caplog.records])
        assert len(metadata) == 0
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any(
            "exiftool returned an empty string" in record.message
            for record in caplog.records
        )

    def test_json_decode_error(self, monkeypatch, caplog):
        """
        AsyncExifTool logs a warning if it cannot decode the exiftool response
        """

        async def nop_cse(*_, **__):
            loop = asyncio.events.get_event_loop()
            loop.set_debug(True)
            return AsyncNopProcess(
                stdout_messages=(
                    b"{,}\n",
                    b"{ready}",
                )
            )

        monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
        caplog.set_level(logging.DEBUG)
        metadata = AsyncExifTool(batch_size=10).get_metadata_batch(["asdf.bin"])
        print([(r.levelname, r) for r in caplog.records])
        assert len(metadata) == 0
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any("JSONDecodeError" in record.message for record in caplog.records)

    def test_type_error(self, monkeypatch, caplog):
        """
        AsyncExifTool expects a JSON list of dicts, not a dict
        """

        async def nop_cse(*_, **__):
            loop = asyncio.events.get_event_loop()
            loop.set_debug(True)
            return AsyncNopProcess(
                stdout_messages=(
                    b'{"SourceFile":"asdf.bin"}\n',
                    b"{ready}",
                )
            )

        monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
        caplog.set_level(logging.DEBUG)
        metadata = AsyncExifTool(batch_size=10).get_metadata_batch(["asdf.bin"])
        print([(r.levelname, r) for r in caplog.records])
        print(metadata)
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any("TypeError" in record.message for record in caplog.records)
        assert len(metadata) == 0

    def test_no_sourcefile(self, monkeypatch, caplog):
        """
        AsyncExifTool expects each file metadata to have a SourceFile key
        and will log a warning if none is found.
        """

        async def nop_cse(*_, **__):
            loop = asyncio.events.get_event_loop()
            loop.set_debug(True)
            return AsyncNopProcess(
                stdout_messages=(
                    b'[{"EXIF:key":"data"}]\n',
                    b"{ready}",
                )
            )

        monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
        caplog.set_level(logging.DEBUG)
        metadata = AsyncExifTool(batch_size=10).get_metadata_batch(["asdf.bin"])
        print([(r.levelname, r) for r in caplog.records])
        print(metadata)
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any("no SourceFile" in record.message for record in caplog.records)
        assert len(metadata) == 0

    def test_interrupt(self, monkeypatch):
        """
        If AsyncPyExifTool workers are terminated early,
        the program should exit without delay.
        """

        async def nop_cse(*_, **__):
            loop = asyncio.events.get_event_loop()
            loop.set_debug(True)
            return AsyncNopProcess(
                stdout_messages=(
                    b'{"SourceFile":"asdf.bin"}\n',
                    b"{ready}",
                ),
                message_delay=5,
            )

        monkeypatch.setattr(asyncio.subprocess, "create_subprocess_exec", nop_cse)
        tool = AsyncExifTool(batch_size=10)

        queue_join = asyncio.Queue.join

        async def join(q_self: asyncio.Queue):
            await asyncio.sleep(0.01)
            tool.terminate()
            await asyncio.wait_for(queue_join(q_self), timeout=1.0)

        monkeypatch.setattr(asyncio.Queue, "join", join)
        all_jobs = [
            ExifToolJob(
                params=[b"img.jpg"],
            )
        ]
        metadata = asyncio.run(tool.execute_queue(all_jobs=all_jobs))
        assert len(metadata) == 0
        assert len(tool.workers) == 0


class TestPyExifTool:
    expected_metadata = [
        {
            "filename": "img1.jpg",
            "exiftool_output": "[{}]",
            "value": {},
        },
        {
            "filename": "img1.jpg",
            "exiftool_output": '[{"SourceFile":"img1.jpg"}]',
            "value": {"SourceFile": "img1.jpg"},
        },
    ]

    @pytest.mark.parametrize("metadata", expected_metadata)
    def test_get_metadata(self, metadata, caplog):
        caplog.set_level(logging.DEBUG)
        exiftool = ExifTool(executable_="true")
        exiftool._process = NopProcess(
            stdout_messages=(
                metadata["exiftool_output"].encode(),
                b"{ready}",
            )
        )
        exiftool.running = True
        assert exiftool.get_metadata(filename=metadata["filename"]) == metadata["value"]

    def test_get_metadata_batch(self, caplog):
        caplog.set_level(logging.DEBUG)
        exiftool = ExifTool(executable_="true")
        exiftool._process = NopProcess(
            stdout_messages=(
                b'[{"SourceFile":"img1.jpg"},{"SourceFile":"img2.jpg"}]',
                b"{ready}",
            )
        )
        exiftool.running = True
        assert exiftool.get_metadata_batch(filenames=["img1.jpg", "img2.jpg"]) == [
            {"SourceFile": "img1.jpg"},
            {"SourceFile": "img2.jpg"},
        ]
        with pytest.raises(TypeError):
            exiftool.get_tags_batch(None, "img1.jpg")
        with pytest.raises(TypeError):
            exiftool.get_tags_batch("EXIF:DateTimeOriginal", None)

    def test_execute_json(self, caplog):
        """
        execute_json logs a warning if a string is empty and
        raises a JsonDecodeError if it cannot decode
        """
        exiftool = ExifTool()
        exiftool.execute = lambda *args: b"\n"
        assert exiftool.execute_json() == []
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any("empty string" in record.message for record in caplog.records)
        caplog.records.clear()
        exiftool.execute = lambda *args: b'[{"a":"b"},{"c":"d"}]'
        assert exiftool.execute_json() == [{"a": "b"}, {"c": "d"}]
        assert not any(record.levelname == "WARNING" for record in caplog.records)
        caplog.records.clear()
        exiftool.execute = lambda *args: b"[{]"
        with pytest.raises(orjson.JSONDecodeError):
            exiftool.execute_json()

    def test_parse_get_metadata(self):
        """
        get_metadata is execute_json's first element, or None
        """
        exiftool = ExifTool()
        exiftool.execute_json = lambda *args: []
        assert exiftool.get_metadata("a.jpg") == {}
        exiftool.execute_json = lambda *args: [{"a": "b"}, {"c": "d"}]
        assert exiftool.get_metadata("a.jpg") == {"a": "b"}

    def test_parse_get_tags_batch(self, caplog):
        """
        get_tags_batch logs a warning if execute_json returns the wrong
        number of arguments
        """
        exiftool = ExifTool()
        exiftool.execute_json = lambda *args: []
        assert exiftool.get_tags_batch((), ("a.jpg",)) == []
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any("bad response" in record.message for record in caplog.records)
        caplog.records.clear()

        exiftool.execute_json = lambda *args: [{"a": "b"}, {"c": "d"}]
        assert exiftool.get_tags_batch((), ("a.jpg",)) == [{"a": "b"}, {"c": "d"}]
        assert any(record.levelname == "WARNING" for record in caplog.records)
        assert any("bad response" in record.message for record in caplog.records)
        caplog.records.clear()

        exiftool.execute_json = lambda *args: [{"a": "b"}]
        assert exiftool.get_tags_batch((), ("a.jpg",)) == [{"a": "b"}]
        assert not any(record.levelname == "WARNING" for record in caplog.records)
        assert not any("bad response" in record.message for record in caplog.records)
        caplog.records.clear()

    def test_parse_get_tags(self):
        """
        get_tags is get_tags_batch's first element, or {}
        """
        exiftool = ExifTool()
        exiftool.get_tags_batch = lambda *args: []
        assert exiftool.get_tags(("E",), "a.jpg") == {}
        exiftool.get_tags_batch = lambda *args: [{"a": "b"}, {"c": "d"}]
        assert exiftool.get_tags(("E",), "a.jpg") == {"a": "b"}

    def test_parse_get_tag_batch(self):
        """
        get_tag_batch returns the first value in each dict that isn't
        for key SourceFile, or None if there are no values.
        """
        exiftool = ExifTool()
        exiftool.get_tags_batch = lambda *args: []
        assert exiftool.get_tag_batch("E", ("a.jpg",)) == []
        exiftool.get_tags_batch = lambda *args: [{"SourceFile": "a.jpg"}]
        assert exiftool.get_tag_batch("E", ("a.jpg",)) == [None]
        exiftool.get_tags_batch = lambda *args: [
            {"SourceFile": "a.jpg", "E": "c"},
            {"SourceFile": "b.jpg", "E": "f", "G": "h"},
        ]
        assert exiftool.get_tag_batch("E", ("a.jpg", "b.jpg")) == ["c", "f"]

    def test_parse_get_tag(self):
        """
        get_tag is get_tag_batch's first element, or None
        """
        exiftool = ExifTool()
        exiftool.get_tag_batch = lambda *args: []
        assert exiftool.get_tag("E", "a.jpg") is None
        exiftool.get_tag_batch = lambda *args: ["c", "f"]
        assert exiftool.get_tag("E", "a.jpg") == "c"

    def test_parse_get_best_datetime_batch(self):
        """
        get_best_datetime_batch gets the best datetime from get_tags_batch
        """
        exiftool = ExifTool()
        exiftool.get_tags_batch = lambda *args: []
        assert exiftool.get_best_datetime_batch(("a.jpg", "b.jpg")) == []
        exiftool.get_tags_batch = lambda *args: [
            {
                "SourceFile": "/images/img8.MP4",
                "File:FileCreateDate": "2020:05:20 12:39:39-04:00",
                "File:FileModifyDate": "2020:05:20 12:39:39-04:00",
            },
            {
                "SourceFile": "/images/img7.HEIC",
                "EXIF:CreationDate": "2021:02:08 21:45:02",
                "XMP:CreateDate": "2021:02:08 21:45:01",
                "File:FileCreateDate": "2021:02:08 23:19:05-05:00",
                "File:FileModifyDate": "2021:02:08 23:19:05-05:00",
            },
        ]
        assert exiftool.get_best_datetime_batch(
            ("/images/img8.MP4", "/images/img7.HEIC")
        ) == ["2020:05:20 12:39:39-04:00", "2021:02:08 21:45:02"]

    def test_parse_get_best_datetime(self):
        """
        get_best_datetime is get_best_datetime_batch's first element, or None
        """
        exiftool = ExifTool()
        exiftool.get_best_datetime_batch = lambda *args: []
        assert exiftool.get_best_datetime(("a.jpg", "b.jpg")) is None
        exiftool.get_best_datetime_batch = lambda *args: ["2020:05:20", "2021:02:08"]
        assert exiftool.get_best_datetime(("a.jpg", "b.jpg")) == "2020:05:20"
