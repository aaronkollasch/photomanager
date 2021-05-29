from click.testing import CliRunner
import json
from photomanager import photomanager, database
from pathlib import Path


def test_database_create(tmpdir):
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
