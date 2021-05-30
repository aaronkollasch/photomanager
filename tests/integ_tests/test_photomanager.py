import json
import os
from pathlib import Path
import pytest
from click.testing import CliRunner
from photomanager import photomanager, database

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
    "B/img1.jpg": "d090ce7023b57925e7e94fc80372e3434fb1897e00b4452a25930dd1b83648fb",
    "B/img2.jpg": "e9fec87008fd240309b81c997e7ec5491fee8da7eb1a76fc39b8fcafa76bb583",
    "C/img3.tiff": "2aca4e78afbcebf2526ad8ac544d90b92991faae22499eec45831ef7be392391",
}


def test_photomanager_create(tmpdir):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmpdir) as td:
        result = runner.invoke(photomanager.main, ["create", "--db", "test.json"])
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


@ALL_IMG_DIRS
def test_photomanager_import(datafiles):
    runner = CliRunner()
    result = runner.invoke(
        photomanager.main,
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
    print(result.output)
    print(result)
    assert result.exit_code == 0

    with open(datafiles / "test.json", "rb") as f:
        s = f.read()
        db = database.Database.from_json(s)
    print(s.decode("utf-8"))

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("A"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        assert len(photos) == 1
        assert photos[0].source_path == datafiles / rel_path

    result = runner.invoke(
        photomanager.main,
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
    print(result.output)
    print(result)
    assert result.exit_code == 0

    with open(datafiles / "test.json", "rb") as f:
        s = f.read()
        db = database.Database.from_json(s)
    print(s.decode("utf-8"))

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("B"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        assert len(photos) == 2
        assert photos[1].source_path == datafiles / rel_path

    result = runner.invoke(
        photomanager.main,
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
    print(result.output)
    print(result)
    assert result.exit_code == 0

    with open(datafiles / "test.json", "rb") as f:
        s = f.read()
        db = database.Database.from_json(s)
    print(s.decode("utf-8"))

    for rel_path, checksum in EXPECTED_HASHES.items():
        if not rel_path.startswith("C"):
            continue
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        assert len(photos) == 1
        assert photos[0].source_path == datafiles / rel_path

    os.makedirs(datafiles / "pm_store", exist_ok=True)
    result = runner.invoke(
        photomanager.main,
        [
            "collect",
            "--db",
            str(datafiles / "test.json"),
            "--destination",
            str(datafiles / "pm_store"),
            "--debug",
        ],
    )
    print(result.output)
    print(result)
    assert result.exit_code == 0

    print(list(Path(datafiles / "pm_store").glob("**/*")))

    with open(datafiles / "test.json", "rb") as f:
        s = f.read()
        db = database.Database.from_json(s)
    print(s.decode("utf-8"))

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
        f.seek(0)
        f.write(db.json)
        f.truncate()

    result = runner.invoke(
        photomanager.main,
        [
            "collect",
            "--db",
            str(datafiles / "test.json"),
            "--destination",
            str(datafiles / "pm_store"),
            "--debug",
        ],
    )
    print(result.output)
    print(result)
    assert result.exit_code == 1

    with open(datafiles / "test.json", "rb") as f:
        s = f.read()
        db = database.Database.from_json(s)
    print(s.decode("utf-8"))

    for rel_path, checksum in EXPECTED_HASHES.items():
        assert checksum in db.hash_to_uid
        assert db.hash_to_uid[checksum] in db.photo_db
        photos = db.photo_db[db.hash_to_uid[checksum]]
        if rel_path == "A/img2.jpg":
            assert photos[0].source_path == datafiles / rel_path
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


@ALL_IMG_DIRS
def test_photomanager_verify(datafiles):
    runner = CliRunner()
    os.makedirs(datafiles / "pm_store", exist_ok=True)
    result = runner.invoke(
        photomanager.main,
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
    print("import")
    print(result.output)
    assert result.exit_code == 0

    result = runner.invoke(
        photomanager.main,
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
    print("verify")
    print(result.output)
    assert result.exit_code == 0

    result = runner.invoke(
        photomanager.main,
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
    print("verify")
    print(result.output)
    assert result.exit_code == 0

    file_to_mod = next(iter(Path(datafiles / "pm_store").glob("**/*.*")))
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
        photomanager.main,
        [
            "verify",
            "--db",
            str(datafiles / "test.json"),
            "--destination",
            str(datafiles / "pm_store"),
        ],
    )
    print("verify incorrect")
    print(result.output)
    assert result.exit_code == 1

    os.remove(file_to_mod)
    result = runner.invoke(
        photomanager.main,
        [
            "verify",
            "--db",
            str(datafiles / "test.json"),
            "--destination",
            str(datafiles / "pm_store"),
        ],
    )
    print("verify missing")
    print(result.output)
    assert result.exit_code == 1
