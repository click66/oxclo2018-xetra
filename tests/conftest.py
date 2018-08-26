import os
import pytest
from spark import get_spark


@pytest.fixture(scope="function")
def spark():
    sc = get_spark()
    sc.setLogLevel("WARN")
    yield sc
    sc.stop()


@pytest.fixture(scope="session")
def data_dir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "_data"))
