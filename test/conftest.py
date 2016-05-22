import os.path
import sys

sys.path.append(os.path.dirname(__file__))  # noqa

from .fixtures.sqlite import *  # noqa
from .fixtures.postgresql import *  # noqa
