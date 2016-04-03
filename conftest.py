import os.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'test', 'lib'))  # noqa

from fixtures.sqlite import *  # noqa
from fixtures.postgresql import *  # noqa
