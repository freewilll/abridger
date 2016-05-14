class TestSqlitePostgresSchemaEquivalence(object):
    def test_equality(self, schema1_sl, schema1_pg):
        sl_table_names = set([t.name for t in schema1_sl.tables])
        pg_table_names = set([t.name for t in schema1_pg.tables])
        assert sl_table_names == pg_table_names

        for table_name in pg_table_names:
            sl_table = schema1_sl.tables_by_name[table_name]
            pg_table = schema1_pg.tables_by_name[table_name]

            sl_col_names = set([c.name for c in sl_table.cols])
            pg_col_names = set([c.name for c in pg_table.cols])
            assert sl_col_names == pg_col_names

            def remove_name_from_relations(relations):
                for r in relations:
                    del r['name']

            sl_relations = remove_name_from_relations(schema1_sl.relations())
            pg_relations = remove_name_from_relations(schema1_pg.relations())

            assert sl_relations == pg_relations
