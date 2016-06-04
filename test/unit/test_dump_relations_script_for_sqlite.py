from minime.dump_relations import main


class TestDumpRelationsScriptForSqlite(object):
    def test_main(self, capsys, sqlite_dbconn):
        # This doesn't test the data itself, just the executable.
        url = 'sqlite://%s' % (sqlite_dbconn.path)
        sqlite_dbconn.disconnect()
        main([url])
        out, err = capsys.readouterr()
