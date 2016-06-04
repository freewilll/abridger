from minime.dump_relations import main


class TestDumpRelationsScriptForSqlite(object):
    def test_main(self, capsys, sqlite_database):
        # This doesn't test the data itself, just the executable.
        url = 'sqlite://%s' % (sqlite_database.path)
        sqlite_database.disconnect()
        main([url])
        out, err = capsys.readouterr()
