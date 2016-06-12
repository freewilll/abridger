import pytest


from abridger.dump_relations import main
from test.conftest import got_postgresql


@pytest.mark.skipif(not got_postgresql(), reason='Needs postgresql')
class TestDumpRelationsScriptForPostgresql(object):
    def test_main(self, capsys, postgresql_database):
        # This doesn't test the data itself, just the executable.
        url = 'postgresql://%s@%s:%s/%s' % (
            postgresql_database.user,
            postgresql_database.host,
            postgresql_database.port,
            postgresql_database.dbname)
        postgresql_database.disconnect()
        main([url])
        out, err = capsys.readouterr()
