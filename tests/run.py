import os
import pytest
import sys

rootdir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

sys.path.insert(0, os.path.abspath(os.path.join(rootdir, "xetra_analyser")))

sys.exit(pytest.main([os.path.join(rootdir, "tests")]))
