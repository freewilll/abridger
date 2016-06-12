from pymlconf import ConfigManager
import importlib
import os.path
import pytest
import subprocess
import sys

sys.path.append(os.path.dirname(__file__))  # noqa

from .fixtures.sqlite import *  # noqa
from .fixtures.postgresql import *  # noqa


def got_postgresql():
    # This contains copy-pasted code from the dbfixtures pytest module.
    # I know this considered bad practice, but there was no way to obtain
    # the information by using existing methods in the module, hence the
    # duplication.

    try:
        importlib.import_module('psycopg2')
    except ImportError:
        return False

    # Adapted from utils.get_config
    config_name = pytest.config.getvalue('db_conf')
    config = ConfigManager(files=[config_name])

    # Adapted from factories.postgresql.postgresql_proc_fixture
    postgresql_ctl = config.postgresql.postgresql_ctl
    if not os.path.exists(postgresql_ctl):
        try:
            pg_bindir = subprocess.check_output(
                ['pg_config', '--bindir'], universal_newlines=True
            ).strip()
        except FileNotFoundError:
            return False
        postgresql_ctl = os.path.join(pg_bindir, 'pg_ctl')

    return True
