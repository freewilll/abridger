from minime.dump_relations import main


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
