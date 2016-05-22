import os
import sys

sys.path.insert(0, os.path.abspath('bin'))  # noqa
from dump_relations import main


class TestDumpRelationsScriptForPostgresql(object):
    def test_main(self, capsys, postgresql_dbconn):
        # This doesn't test the data itself, just the executable.
        url = 'postgresql://%s@%s:%s/%s' % (
            postgresql_dbconn.user,
            postgresql_dbconn.host,
            postgresql_dbconn.port,
            postgresql_dbconn.dbname)
        postgresql_dbconn.disconnect()
        main([url])
        out, err = capsys.readouterr()
