import importlib.util
import pathlib

import pytest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "futures_updater.py"


@pytest.fixture(scope="session")
def fut():
    spec = importlib.util.spec_from_file_location("futures_updater", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
