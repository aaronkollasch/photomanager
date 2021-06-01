import json
import logging
import os
from pathlib import Path
import subprocess
import pytest
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


def test_photomanager_bin_install():
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


def test_photomanager_bin_error(tmpdir):
    p = subprocess.Popen(
        ["photomanager", "stats", "--db", str(tmpdir / "none.json")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate()
    print(stdout, stderr)
    print("exit", p.returncode)
    assert p.returncode == 1
    assert b"FileNotFoundError" in stderr


def test_cli_create(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as td:
        result = runner.invoke(cli.main, ["create", "--db", "test.json"])
        print(result.output)
        print(list(Path(td).glob("**/*")))
        assert result.exit_code == 0
        with open(Path(td) / "test.json") as f:
            s = f.read()
        print(s)
        d = json.loads(s)
        assert d["photo_db"] == {}
        assert d["version"] == database.Database.VERSION
        assert d["hash_algorithm"] == database.DEFAULT_HASH_ALGO


def test_cli_index_directory_db(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    result = runner.invoke(
        cli.main,
        [
            "index",
            "--db",
            str(tmpdir),
        ],
    )
    print("\nINDEX directory db")
    print(result.output)
    print(result)
    assert result.exit_code == 2
    assert "is a directory" in result.output


def test_cli_index_nothing(tmpdir, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    result = runner.invoke(
        cli.main,
        [
            "index",
            "--db",
            str(tmpdir / "test1.json"),
            "--priority",
            "10",
        ],
    )
    print("\nINDEX nothing")
    print(result.output)
    print(result)
    assert result.exit_code == 1
    assert "Nothing to index" in result.output


@ALL_IMG_DIRS
def test_cli_import(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    result = runner.invoke(
        cli.main,
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
    print(db.db)

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("A"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        assert len(photos) == 1
        assert photos[0].source_path == datafiles / rel_path

    result = runner.invoke(
        cli.main,
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
    print(db.db)

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("B"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        assert len(photos) == 2
        assert photos[1].source_path == datafiles / rel_path

    caplog.set_level(logging.INFO)
    s_prev = s
    result = runner.invoke(
        cli.main,
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
        cli.main,
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
    print(db.db)

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("C"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        assert len(photos) == 1
        assert photos[0].source_path == datafiles / rel_path

    db_prev = db
    result = runner.invoke(
        cli.main,
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
    print(db.db)
    assert db.photo_db == db_prev.photo_db

    os.makedirs(datafiles / "pm_store", exist_ok=True)
    result = runner.invoke(
        cli.main,
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
    print(db.db)

    collected_dbs = list(Path(datafiles / "pm_store" / "database").glob("test*.json"))
    assert len(collected_dbs) == 1
    with open(collected_dbs[0], "rb") as f:
        assert f.read() == s

    for rel_path, checksum in EXPECTED_HASHES.items():
        abs_path = datafiles / rel_path
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        if rel_path.startswith("A"):
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path != ""
            assert abs_path.exists()
            assert (datafiles / "pm_store" / photos[0].store_path).exists()
        elif rel_path.startswith("B"):
            assert len(photos) == 2
            assert photos[1].source_path == datafiles / rel_path
            assert photos[1].store_path == ""
        elif rel_path.startswith("C"):
            assert len(photos) == 1
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path != ""
            assert abs_path.exists()
            assert (datafiles / "pm_store" / photos[0].store_path).exists()

    result = runner.invoke(
        cli.main,
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
            if pf.store_path and pf.checksum == EXPECTED_HASHES["A/img1.jpg"]
        )
        img1_jpg.store_path = ""
        img2_jpg = next(
            pf
            for photos in db.photo_db.values()
            for pf in photos
            if pf.store_path and pf.checksum == EXPECTED_HASHES["A/img2.jpg"]
        )
        os.remove(img2_jpg.source_path)
        os.remove(datafiles / "pm_store" / img2_jpg.store_path)
        img2_png = next(
            pf
            for photos in db.photo_db.values()
            for pf in photos
            if pf.store_path and pf.checksum == EXPECTED_HASHES["A/img1.png"]
        )
        os.remove(img2_png.source_path)
        img3_tiff = next(
            pf
            for photos in db.photo_db.values()
            for pf in photos
            if pf.store_path and pf.checksum == EXPECTED_HASHES["C/img3.tiff"]
        )
        os.remove(datafiles / "pm_store" / img3_tiff.store_path)
        img4_jpg = next(
            pf
            for photos in db.photo_db.values()
            for pf in photos
            if pf.store_path and pf.checksum == EXPECTED_HASHES["A/img4.jpg"]
        )
        os.remove(img4_jpg.source_path)
        os.remove(datafiles / "pm_store" / img4_jpg.store_path)
        img4_jpg.store_path = ""
        f.seek(0)
        s = db.json
        f.write(s)
        f.truncate()

    s_prev = s
    f_prev = set(Path(datafiles / "pm_store").glob("**/*"))
    result = runner.invoke(
        cli.main,
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
        cli.main,
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
    print(db.db)

    for rel_path, checksum in EXPECTED_HASHES.items():
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        if rel_path == "A/img2.jpg":
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path != ""
            assert not (datafiles / "pm_store" / photos[0].store_path).exists()
        elif rel_path == "A/img4.jpg":
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path == ""
        elif rel_path.startswith("A"):
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path != ""
            assert (datafiles / "pm_store" / photos[0].store_path).exists()
        elif rel_path.startswith("B"):
            assert len(photos) == 2
            assert photos[1].source_path == datafiles / rel_path
            assert photos[1].store_path == ""
        elif rel_path.startswith("C"):
            assert len(photos) == 1
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path != ""
            assert (datafiles / "pm_store" / photos[0].store_path).exists()


@pytest.mark.datafiles(FIXTURE_DIR / "C", keep_top_dir=True)
def test_cli_import_no_overwrite(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    result = runner.invoke(
        cli.main,
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
    print(db.db)

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
        cli.main,
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
    print(db.db)

    assert (
        next(iter(db.photo_db.values()))[0].store_path
        == "2018/08-Aug/2018-08-01_19-28-36-2aca4e7-img3.tiff"
    )

    result = runner.invoke(
        cli.main,
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


@ALL_IMG_DIRS
def test_cli_verify(datafiles, caplog):
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    os.makedirs(datafiles / "pm_store", exist_ok=True)
    result = runner.invoke(
        cli.main,
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
        cli.main,
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
        cli.main,
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
        cli.main,
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
        cli.main,
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
        cli.main,
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


@ALL_IMG_DIRS
def test_cli_clean(datafiles, caplog):
    caplog.set_level(logging.INFO)
    runner = CliRunner()
    os.makedirs(datafiles / "pm_store", exist_ok=True)
    result = runner.invoke(
        cli.main,
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
    print(db.db)

    for rel_path, checksum in EXPECTED_HASHES.items():
        abs_path = datafiles / rel_path
        if not rel_path.startswith("B"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        assert len(photos) == 1
        assert photos[0].source_path == datafiles / rel_path
        assert photos[0].store_path != ""
        assert abs_path.exists()
        assert (datafiles / "pm_store" / photos[0].store_path).exists()

    s_prev = s
    f_prev = set(Path(datafiles / "pm_store").glob("**/*"))
    result = runner.invoke(
        cli.main,
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
        cli.main,
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
    print(db.db)

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("A") and not rel_path.startswith("B"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        if rel_path.startswith("A"):
            assert photos[0].source_path == datafiles / rel_path
            print(rel_path)
            assert photos[0].store_path != ""
            assert (datafiles / "pm_store" / photos[0].store_path).exists()
        elif rel_path.startswith("B"):
            assert len(photos) == 2
            assert photos[1].source_path == datafiles / rel_path
            assert photos[1].store_path != ""
            assert (datafiles / "pm_store" / photos[1].store_path).exists()

    assert any(
        p.name == "2018-08-01_20-28-36-2b0f304-img4.jpg"
        for p in Path(datafiles / "pm_store").glob("**/*.*")
    )
    result = runner.invoke(
        cli.main,
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
    print(db.db)

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("A") and not rel_path.startswith("B"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        if rel_path == "A/img4.jpg":
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path != ""
            assert (datafiles / "pm_store" / photos[0].store_path).exists()
        elif rel_path == "B/img4.jpg":
            assert photos[1].source_path == datafiles / rel_path
            assert photos[1].store_path == ""
        elif rel_path.startswith("A"):
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path != ""
            assert (datafiles / "pm_store" / photos[0].store_path).exists()
        elif rel_path.startswith("B"):
            assert len(photos) == 2
            assert photos[1].source_path == datafiles / rel_path
            assert photos[1].store_path != ""
            assert (datafiles / "pm_store" / photos[1].store_path).exists()

    s_prev = s
    f_prev = set(Path(datafiles / "pm_store").glob("**/*"))
    caplog.set_level(logging.INFO)
    result = runner.invoke(
        cli.main,
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
        cli.main,
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
        cli.main,
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
        cli.main,
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
    print(db.db)
    print(list(Path(datafiles / "pm_store").glob("**/*.*")))

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("A") and not rel_path.startswith("B"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        if rel_path.startswith("A"):
            assert photos[0].source_path == datafiles / rel_path
            assert photos[0].store_path != ""
            assert (datafiles / "pm_store" / photos[0].store_path).exists()
        elif rel_path.startswith("B"):
            assert len(photos) == 2
            assert photos[1].source_path == datafiles / rel_path
            assert photos[1].store_path == ""
