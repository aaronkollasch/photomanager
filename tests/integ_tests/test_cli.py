import logging
import os
import subprocess
from typing import cast
from pathlib import Path
import pytest
from click import Group
from click.testing import CliRunner
from photomanager import cli, database, version

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"
ALL_IMG_DIRS = pytest.mark.datafiles(
    FIXTURE_DIR / "A",
    FIXTURE_DIR / "B",
    FIXTURE_DIR / "C",
    keep_top_dir=True,
)
EXPECTED_HASHES = {
    "A/img1.jpg": "d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
    "A/img2.jpg": "3b39f47d51f63e54c76417ee6e04c34bd3ff5ac47696824426dca9e200f03666",
    "A/img1.png": "1e10df2e3abe4c810551525b6cb2eb805886de240e04cc7c13c58ae208cabfb9",
    "A/img4.jpg": "79ac4a89fb3d81ab1245b21b11ff7512495debca60f6abf9afbb1e1fbfe9d98c",
    "B/img1.jpg": "d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
    "B/img2.jpg": "e9fec87008fd240309b81c997e7ec5491fee8da7eb1a76fc39b8fcafa76bb583",
    "B/img4.jpg": "2b0f304f86655ebd04272cc5e7e886e400b79a53ecfdc789f75dd380cbcc8317",
    "C/img3.tiff": "2aca4e78afbcebf2526ad8ac544d90b92991faae22499eec45831ef7be392391",
}


def check_dir_empty(dir_path):
    cwd_files = list(Path(dir_path).glob("*"))
    print(cwd_files)
    assert len(cwd_files) == 0


def test_photomanager_bin_install(tmpdir):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        p = subprocess.Popen(
            ["photomanager", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        print(stdout, stderr)
        print("exit", p.returncode)
        assert p.returncode == 0
        assert stdout.strip() == f"photomanager {version}".encode()
        check_dir_empty(fs)


def test_photomanager_bin_error(tmpdir):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as fs:
        p = subprocess.Popen(
            ["photomanager", "stats", "--db", str(tmpdir / "none.json")],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        print(stdout, stderr)
        print("exit", p.returncode)
        assert p.returncode == 2
        assert b"does not exist" in stderr
        check_dir_empty(fs)


@ALL_IMG_DIRS
def test_cli_import(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=datafiles) as fs:
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(datafiles / "test.json"),
                "--priority",
                "10",
                "--debug",
                str(datafiles / "A"),
            ],
        )
        print("\nINDEX A")
        print(result.output)
        print(result)
        assert result.exit_code == 0

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        for rel_path, checksum in EXPECTED_HASHES.items():
            if not rel_path.startswith("A"):
                continue
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            assert len(photos) == 1
            assert photos[0].src == datafiles / rel_path

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(datafiles / "test.json"),
                "--priority",
                "20",
                "--debug",
                str(datafiles / "B"),
            ],
        )
        print("\nINDEX B")
        print(result.output)
        print(result)
        assert result.exit_code == 0

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        for rel_path, checksum in EXPECTED_HASHES.items():
            if not rel_path.startswith("B"):
                continue
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            assert len(photos) == 2
            assert photos[1].src == datafiles / rel_path

        caplog.set_level(logging.INFO)
        s_prev = s
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(datafiles / "test.json"),
                "--priority",
                "10",
                "--dry-run",
                str(datafiles / "C"),
            ],
        )
        print("\nINDEX C dry-run")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        caplog.set_level(logging.DEBUG)

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            assert s == s_prev

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(datafiles / "test.json"),
                "--priority",
                "10",
                "--debug",
                str(datafiles / "C"),
            ],
        )
        print("\nINDEX C")
        print(result.output)
        print(result)
        assert result.exit_code == 0

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        for rel_path, checksum in EXPECTED_HASHES.items():
            if not rel_path.startswith("C"):
                continue
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            assert len(photos) == 1
            assert photos[0].src == datafiles / rel_path

        db_prev = db
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(datafiles / "test.json"),
                "--priority",
                "10",
                "--debug",
                str(datafiles / "C"),
            ],
        )
        print("\nINDEX C re-run")
        print(result.output)
        print(result)
        assert result.exit_code == 0

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)
        assert db.photo_db == db_prev.photo_db

        os.makedirs(datafiles / "pm_store", exist_ok=True)
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "collect",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--collect-db",
                "--debug",
            ],
        )
        print("\nCOLLECT")
        print(result.output)
        print(result)
        assert result.exit_code == 0

        print(list(Path(datafiles / "pm_store").glob("**/*")))

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        collected_dbs = list(
            Path(datafiles / "pm_store" / "database").glob("test*.json")
        )
        assert len(collected_dbs) == 1
        with open(collected_dbs[0], "rb") as f:
            assert f.read() == s

        for rel_path, checksum in EXPECTED_HASHES.items():
            abs_path = datafiles / rel_path
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            if rel_path.startswith("A"):
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto != ""
                assert abs_path.exists()
                assert (datafiles / "pm_store" / photos[0].sto).exists()
            elif rel_path.startswith("B"):
                assert len(photos) == 2
                assert photos[1].src == datafiles / rel_path
                assert photos[1].sto == ""
            elif rel_path.startswith("C"):
                assert len(photos) == 1
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto != ""
                assert abs_path.exists()
                assert (datafiles / "pm_store" / photos[0].sto).exists()

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "stats",
                "--db",
                str(datafiles / "test.json"),
            ],
        )
        print("\nSTATS")
        print(repr(result.output))
        print(result)
        assert result.exit_code == 0
        assert result.output == (
            "Total items:        8\n"
            "Total unique items: 5\n"
            "Total stored items: 5\n"
            "Total file size:    3 kB\n"
        )

        # Test behavior if photos are missing or marked as not stored
        with open(datafiles / "test.json", "r+b") as f:
            s = f.read()
            db = database.Database.from_json(s)
            img1_jpg = next(
                pf
                for photos in db.photo_db.values()
                for pf in photos
                if pf.sto and pf.chk == EXPECTED_HASHES["A/img1.jpg"]
            )
            img1_jpg.sto = ""
            img2_jpg = next(
                pf
                for photos in db.photo_db.values()
                for pf in photos
                if pf.sto and pf.chk == EXPECTED_HASHES["A/img2.jpg"]
            )
            os.remove(img2_jpg.src)
            os.remove(datafiles / "pm_store" / img2_jpg.sto)
            img2_png = next(
                pf
                for photos in db.photo_db.values()
                for pf in photos
                if pf.sto and pf.chk == EXPECTED_HASHES["A/img1.png"]
            )
            os.remove(img2_png.src)
            img3_tiff = next(
                pf
                for photos in db.photo_db.values()
                for pf in photos
                if pf.sto and pf.chk == EXPECTED_HASHES["C/img3.tiff"]
            )
            os.remove(datafiles / "pm_store" / img3_tiff.sto)
            img4_jpg = next(
                pf
                for photos in db.photo_db.values()
                for pf in photos
                if pf.sto and pf.chk == EXPECTED_HASHES["A/img4.jpg"]
            )
            os.remove(img4_jpg.src)
            os.remove(datafiles / "pm_store" / img4_jpg.sto)
            img4_jpg.sto = ""
            f.seek(0)
            s = db.to_json(pretty=True)
            f.write(s)
            f.truncate()

        s_prev = s
        f_prev = set(Path(datafiles / "pm_store").glob("**/*"))
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "collect",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--dry-run",
                "--debug",
            ],
        )
        print("\nCOLLECT dry-run")
        print(result.output)
        print(result)
        assert result.exit_code == 1

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            assert s == s_prev
        assert set(Path(datafiles / "pm_store").glob("**/*")) == f_prev

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "collect",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--debug",
            ],
        )
        print("\nCOLLECT missing")
        print(result.output)
        print(result)
        assert result.exit_code == 1

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        for rel_path, checksum in EXPECTED_HASHES.items():
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            if rel_path == "A/img2.jpg":
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto != ""
                assert not (datafiles / "pm_store" / photos[0].sto).exists()
            elif rel_path == "A/img4.jpg":
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto == ""
            elif rel_path.startswith("A"):
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto != ""
                assert (datafiles / "pm_store" / photos[0].sto).exists()
            elif rel_path.startswith("B"):
                assert len(photos) == 2
                assert photos[1].src == datafiles / rel_path
                assert photos[1].sto == ""
            elif rel_path.startswith("C"):
                assert len(photos) == 1
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto != ""
                assert (datafiles / "pm_store" / photos[0].sto).exists()
        check_dir_empty(fs)


@pytest.mark.datafiles(FIXTURE_DIR / "C", keep_top_dir=True)
def test_cli_import_no_overwrite(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=datafiles) as fs:
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(datafiles / "test.json"),
                "--priority",
                "10",
                "--debug",
                str(datafiles / "C"),
            ],
        )
        print("\nINDEX C")
        print(result.output)
        print(result)
        assert result.exit_code == 0

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        os.makedirs(datafiles / "pm_store" / "2018" / "08-Aug", exist_ok=True)
        with open(
            datafiles
            / "pm_store"
            / "2018"
            / "08-Aug"
            / "2018-08-01_19-28-36-2aca4e7-img3.tiff",
            "w",
        ) as f:
            f.write("test_message")

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "collect",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--collect-db",
                "--debug",
            ],
        )
        print("\nCOLLECT")
        print(result.output)
        print(result)
        assert result.exit_code == 0

        print(list(Path(datafiles / "pm_store").glob("**/*")))
        with open(
            datafiles
            / "pm_store"
            / "2018"
            / "08-Aug"
            / "2018-08-01_19-28-36-2aca4e7-img3.tiff",
            "r",
        ) as f:
            assert f.read() == "test_message"

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        assert (
            next(iter(db.photo_db.values()))[0].sto
            == "2018/08-Aug/2018-08-01_19-28-36-2aca4e7-img3.tiff"
        )

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "verify",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
            ],
        )
        print("\nVERIFY")
        print(result.output)
        print(result)
        assert result.exit_code == 1

        check_dir_empty(fs)


@pytest.mark.datafiles(FIXTURE_DIR / "C", keep_top_dir=True)
def test_cli_index_skip_existing(datafiles, caplog):
    """
    The --skip-existing flag prevents indexing existing source files
    """
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=datafiles) as fs:
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(datafiles / "test.json"),
                "--priority",
                "10",
                "--debug",
                str(datafiles / "C"),
            ],
        )
        print("\nINDEX C")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert "Indexed 1/1 items" in caplog.messages
        assert "Added 1 new items and merged 0 items" in caplog.messages

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)
        assert sum(1 for _ in db.sources) == 1
        assert set(db.sources) == {str(datafiles / "C" / "img3.tiff")}

        with open(datafiles / "C" / "newphoto.jpg", "wb") as f:
            f.write(b"contents")

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "index",
                "--db",
                str(datafiles / "test.json"),
                "--priority",
                "10",
                "--skip-existing",
                "--debug",
                str(datafiles / "C"),
            ],
        )
        print("\nINDEX C skip-existing")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert "Indexed 1/1 items" in caplog.messages
        assert "Added 1 new items and merged 0 items" in caplog.messages

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)
        assert sum(1 for _ in db.sources) == 2
        assert set(db.sources) == {
            str(datafiles / "C" / "img3.tiff"),
            str(datafiles / "C" / "newphoto.jpg"),
        }

        check_dir_empty(fs)


@pytest.mark.datafiles(FIXTURE_DIR / "C", keep_top_dir=True)
def test_cli_import_skip_existing(datafiles, caplog):
    """
    The --skip-existing flag prevents indexing existing source files
    but not collecting them
    """
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=datafiles) as fs:
        os.makedirs(datafiles / "dest")
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "dest"),
                "--priority",
                "10",
                "--debug",
                str(datafiles / "C"),
            ],
        )
        print("\nIMPORT C")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert "Indexed 1/1 items" in caplog.messages
        assert "Added 1 new items and merged 0 items" in caplog.messages
        assert any("Copied 1 items" in m for m in caplog.messages)
        imported_files = list(Path(datafiles / "dest").glob("**/*.*"))
        assert len(imported_files) == 1
        os.remove(imported_files[0])

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)
        assert sum(1 for _ in db.sources) == 1
        assert set(db.sources) == {str(datafiles / "C" / "img3.tiff")}

        with open(datafiles / "C" / "newphoto.jpg", "wb") as f:
            f.write(b"contents")

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "dest"),
                "--priority",
                "10",
                "--skip-existing",
                "--debug",
                str(datafiles / "C"),
            ],
        )
        print("\nINDEX C skip-existing")
        print(result.output)
        print(result)
        assert result.exit_code == 0
        assert "Indexed 1/1 items" in caplog.messages
        assert "Added 1 new items and merged 0 items" in caplog.messages
        assert any("Copied 2 items" in m for m in caplog.messages)
        imported_files = list(Path(datafiles / "dest").glob("**/*.*"))
        assert len(imported_files) == 2

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)
        assert sum(1 for _ in db.sources) == 2
        assert set(db.sources) == {
            str(datafiles / "C" / "img3.tiff"),
            str(datafiles / "C" / "newphoto.jpg"),
        }

        check_dir_empty(fs)


@ALL_IMG_DIRS
def test_cli_verify(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=datafiles) as fs:
        os.makedirs(datafiles / "pm_store", exist_ok=True)
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--priority",
                "10",
                "--collect-db",
                "--debug",
                str(datafiles / "A"),
            ],
        )
        print("\nIMPORT")
        print(result.output)
        assert result.exit_code == 0

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "verify",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--storage-type",
                "SSD",
            ],
        )
        print("\nVERIFY")
        print(result.output)
        assert result.exit_code == 0

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "verify",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--subdir",
                "2018",
            ],
        )
        print("\nVERIFY subdir")
        print(result.output)
        assert result.exit_code == 0

        file_to_mod = next(iter(Path(datafiles / "pm_store").glob("**/*.jpg")))
        file_to_mod.chmod(0o666)
        with open(file_to_mod, "r+b") as f:
            print(file_to_mod)
            pos = len(f.read()) - 20
            f.seek(pos)
            c = f.read(1)
            f.seek(pos)
            f.write(bytes([ord(c) ^ 0b1]))
        file_to_mod.chmod(0o444)

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "verify",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
            ],
        )
        print("\nVERIFY incorrect")
        print(result.output)
        assert result.exit_code == 1

        os.remove(file_to_mod)
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "verify",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
            ],
        )
        print("\nVERIFY missing")
        print(result.output)
        assert result.exit_code == 1

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "verify",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--storage-type",
                "RAID",
            ],
        )
        print("\nVERIFY missing async")
        print(result.output)
        assert result.exit_code == 1

        check_dir_empty(fs)


@ALL_IMG_DIRS
def test_cli_clean(datafiles, caplog):
    caplog.set_level(logging.INFO)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=datafiles) as fs:
        os.makedirs(datafiles / "pm_store", exist_ok=True)
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--priority",
                "20",
                "--storage-type",
                "RAID",
                str(datafiles / "B"),
            ],
        )
        print("\nIMPORT B")
        print(result.output)
        assert result.exit_code == 0
        caplog.set_level(logging.DEBUG)

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        for rel_path, checksum in EXPECTED_HASHES.items():
            abs_path = datafiles / rel_path
            if not rel_path.startswith("B"):
                continue
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            assert len(photos) == 1
            assert photos[0].src == datafiles / rel_path
            assert photos[0].sto != ""
            assert abs_path.exists()
            assert (datafiles / "pm_store" / photos[0].sto).exists()

        s_prev = s
        f_prev = set(Path(datafiles / "pm_store").glob("**/*"))
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--priority",
                "10",
                "--dry-run",
                "--debug",
                str(datafiles / "A"),
            ],
        )
        print("\nIMPORT A dry-run")
        print(result.output)
        assert result.exit_code == 0

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            assert s == s_prev
        assert set(Path(datafiles / "pm_store").glob("**/*")) == f_prev

        result = runner.invoke(
            cast(Group, cli.main),
            [
                "import",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--priority",
                "10",
                "--debug",
                str(datafiles / "A"),
            ],
        )
        print("\nIMPORT A")
        print(result.output)
        assert result.exit_code == 0

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        for rel_path, checksum in EXPECTED_HASHES.items():
            if not rel_path.startswith("A") and not rel_path.startswith("B"):
                continue
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            if rel_path.startswith("A"):
                # photos in the /A directory are higher priority
                # and all should have been collected.
                assert photos[0].src == datafiles / rel_path
                print(rel_path)
                assert photos[0].sto != ""
                assert (datafiles / "pm_store" / photos[0].sto).exists()
            elif rel_path.startswith("B"):
                # photos in the /B directory all have alternates in A
                # so 2 photos should be present
                assert len(photos) == 2
                assert photos[1].src == datafiles / rel_path
                assert photos[1].sto != ""
                assert (datafiles / "pm_store" / photos[1].sto).exists()

        assert any(
            p.name == "2018-08-01_20-28-36-2b0f304-img4.jpg"
            for p in Path(datafiles / "pm_store").glob("**/*.*")
        )
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "clean",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--subdir",
                "2018",
                "--debug",
            ],
        )
        print("\nCLEAN subdir")
        print(result.output)
        print(list(Path(datafiles / "pm_store").glob("**/*.*")))
        assert result.exit_code == 0
        assert not any(
            p.name == "2018-08-01_20-28-36-2b0f304-img4.jpg"
            for p in Path(datafiles / "pm_store").glob("**/*.*")
        )

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)

        for rel_path, checksum in EXPECTED_HASHES.items():
            if not rel_path.startswith("A") and not rel_path.startswith("B"):
                continue
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            if rel_path == "A/img4.jpg":
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto != ""
                assert (datafiles / "pm_store" / photos[0].sto).exists()
            elif rel_path == "B/img4.jpg":
                assert photos[1].src == datafiles / rel_path
                assert photos[1].sto == ""
            elif rel_path.startswith("A"):
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto != ""
                assert (datafiles / "pm_store" / photos[0].sto).exists()
            elif rel_path.startswith("B"):
                assert len(photos) == 2
                assert photos[1].src == datafiles / rel_path
                assert photos[1].sto != ""
                assert (datafiles / "pm_store" / photos[1].sto).exists()

        s_prev = s
        f_prev = set(Path(datafiles / "pm_store").glob("**/*"))
        caplog.set_level(logging.INFO)
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "clean",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--dry-run",
            ],
        )
        caplog.set_level(logging.DEBUG)
        print("\nCLEAN dry-run")
        print(result.output)
        assert result.exit_code == 0

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            assert s == s_prev
        assert set(Path(datafiles / "pm_store").glob("**/*")) == f_prev

        os.rename(
            datafiles
            / "pm_store"
            / "2015"
            / "08-Aug"
            / "2015-08-01_18-28-36-e9fec87-img2.jpg",
            datafiles / "temp.jpg",
        )
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "clean",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--dry-run",
                "--debug",
            ],
        )
        print("\nCLEAN dry-run")
        print(result.output)
        assert result.exit_code == 1
        assert "Missing photo" in result.output

        caplog.set_level(logging.INFO)
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "clean",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--dry-run",
            ],
        )
        caplog.set_level(logging.DEBUG)
        print("\nCLEAN dry-run")
        print(result.output)
        assert result.exit_code == 1
        os.rename(
            datafiles / "temp.jpg",
            datafiles
            / "pm_store"
            / "2015"
            / "08-Aug"
            / "2015-08-01_18-28-36-e9fec87-img2.jpg",
        )

        assert any(
            p.name == "2015-08-01_18-28-36-e9fec87-img2.jpg"
            for p in Path(datafiles / "pm_store").glob("**/*.*")
        )
        result = runner.invoke(
            cast(Group, cli.main),
            [
                "clean",
                "--db",
                str(datafiles / "test.json"),
                "--destination",
                str(datafiles / "pm_store"),
                "--debug",
            ],
        )
        print("\nCLEAN")
        print(result.output)
        print(list(Path(datafiles / "pm_store").glob("**/*.*")))
        assert result.exit_code == 0
        assert not any(
            p.name == "2015-08-01_18-28-36-e9fec87-img2.jpg"
            for p in Path(datafiles / "pm_store").glob("**/*.*")
        )

        with open(datafiles / "test.json", "rb") as f:
            s = f.read()
            db = database.Database.from_json(s)
        print(db.json)
        print(list(Path(datafiles / "pm_store").glob("**/*.*")))

        for rel_path, checksum in EXPECTED_HASHES.items():
            if not rel_path.startswith("A") and not rel_path.startswith("B"):
                continue
            assert checksum in db.hash_to_uid
            assert db.hash_to_uid[checksum] in db.photo_db
            photos = db.photo_db[db.hash_to_uid[checksum]]
            if rel_path.startswith("A"):
                assert photos[0].src == datafiles / rel_path
                assert photos[0].sto != ""
                assert (datafiles / "pm_store" / photos[0].sto).exists()
            elif rel_path.startswith("B"):
                assert len(photos) == 2
                assert photos[1].src == datafiles / rel_path
                assert photos[1].sto == ""

        check_dir_empty(fs)
