import importlib
import logging
import sys
from pathlib import Path
from typing import cast

import pytest
from click import Group
from click.testing import CliRunner

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "test_files"


def check_dir_empty(dir_path):
    cwd_files = list(Path(dir_path).glob("*"))
    print(cwd_files)
    assert len(cwd_files) == 0


@pytest.fixture(params=(False, True, False))
def hide_cmi(request, monkeypatch):
    if request.param:
        monkeypatch.setitem(
            sys.modules, "photomanager.check_media_integrity.check_mi", None
        )
    return request.param


@pytest.mark.datafiles(
    FIXTURE_DIR / "A" / "img1.png",
    FIXTURE_DIR / "A" / "img1.jpg",
)
def test_cli_check_integrity_not_available(hide_cmi, datafiles, caplog):
    """
    If importing check_media_integrity gives an ImportError
    and the --check-integrity flag is provided, raise an Exception.
    However, no Exception is raised if --check-integrity is absent.
    """
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=datafiles) as fs:
        from photomanager import cli

        importlib.reload(cli)

        result = runner.invoke(
            cast(Group, cli.main),
            ["index", "--dump", str(datafiles)],
        )
        print(result.output)
        print(result)
        print(result.exception)
        assert not result.exception
        caplog.clear()

        result = runner.invoke(
            cast(Group, cli.main),
            ["index", "--dump", "--check-integrity", str(datafiles)],
        )
        print(result.output)
        print(result)
        print(result.exception)
        if hide_cmi:
            assert result.exception
            assert "check-media-integrity not available:" in str(result.exception)
        else:
            assert not result.exception
        check_dir_empty(fs)
