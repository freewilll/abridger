class TestAlternatePrimaryKeysBase(object):
    test_alternate_primary_keys_sql = [
        '''CREATE TABLE test1 (
                id1 INTEGER UNIQUE
            );''',
        '''CREATE TABLE test2 (
                id1 INTEGER UNIQUE,
                id2 INTEGER UNIQUE
            );''',
        '''CREATE TABLE test3 (
                id1 INTEGER,
                id2 INTEGER,
                id3 INTEGER,
                UNIQUE(id1, id2),
                UNIQUE(id2, id3)
            );''',
        '''CREATE TABLE test4 (
                id1 INTEGER UNIQUE,
                id2 INTEGER,
                id3 INTEGER,
                UNIQUE(id2, id3)
            );''',
        '''CREATE TABLE test5 (
                id1 INTEGER,
                id2 INTEGER,
                id3 INTEGER,
                UNIQUE(id1, id2, id3)
            );''',
        '''CREATE TABLE test6 (
                id1 INTEGER UNIQUE,
                id2 INTEGER,
                id3 INTEGER,
                UNIQUE(id1),
                UNIQUE(id1, id2),
                UNIQUE(id1, id2, id3)
            );''',
        '''CREATE TABLE test7 (
                id1 INTEGER PRIMARY KEY,
                id2 INTEGER,
                id3 INTEGER,
                UNIQUE(id1),
                UNIQUE(id1, id2),
                UNIQUE(id1, id2, id3)
            );''',
    ]

    alternate_primary_keys_tests = [
        ('test1', 1),
        ('test2', 1),
        ('test3', 2),
        ('test4', 1),
        ('test5', 3),
        ('test6', 1),
        ('test7', 0),
    ]

    def check_alternate_primary_keys(self, schema, table, count):
        table = schema.tables_by_name[table]
        assert len(table.alternate_primary_key or []) == count
