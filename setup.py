import setuptools
import sys
from pathlib import Path

sys.path.append((Path(__file__).parent / "build_backend").as_posix())
setuptools.setup()
