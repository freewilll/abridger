import pytest

from abridger.extraction_model import Relation
from abridger.schema import SqliteSchema
from test.unit.rocket.rocket_platform import TestRocketBase


class TestRocketCircularRelations(TestRocketBase):
    @pytest.fixture()
    # A self referencing table
    def schema1(self):
        for sql in [
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                test1_id INTEGER REFERENCES test1
            );''',
        ]:
            self.database.execute(sql)
        return SqliteSchema.create_from_conn(self.database.connection)

    def test_one_table_self_referencing(self, schema1):
        table = schema1.tables[0]

        rows = [(table, (1, 1))]
        self.database.insert_rows(rows)

        extraction_model_data = [
            {'subject': [{'tables': [{'table': 'test1'}]}]},
            {'relations': [{'defaults': Relation.DEFAULT_EVERYTHING}]},
        ]
        self.check_launch(schema1, extraction_model_data, rows)

    @pytest.fixture()
    # A staff-manager example
    def schema2(self):
        for sql in [
            '''CREATE TABLE managers (
                id INTEGER PRIMARY KEY,
                staff_id INTEGER NOT NULL REFERENCES staff
            );''',
            '''CREATE TABLE staff (
                id INTEGER PRIMARY KEY,
                manager_id INTEGER REFERENCES managers
            );''',
        ]:
            self.database.execute(sql)
        return SqliteSchema.create_from_conn(self.database.connection)

    def test_staff_manager(self, schema2):
        managers = schema2.tables[0]
        staff = schema2.tables[1]

        rows = [
            (staff, (1, None)),
            (managers, (1, 1)),
            (staff, (2, 1)),
            (staff, (3, 1)),
        ]
        self.database.insert_rows(rows)

        table = {'table': 'staff', 'column': 'id', 'values': 2}
        extraction_model_data = [
            {'subject': [{'tables': [table]}]},
            {'relations': [{'defaults': Relation.DEFAULT_EVERYTHING}]},
        ]
        self.check_launch(schema2, extraction_model_data, rows)

    @pytest.fixture()
    # A staff-manager example using a foreign key pointing to a non-primary
    # key. This tests the lookup code.
    def schema3(self):
        for sql in [
            '''CREATE TABLE managers (
                id INTEGER PRIMARY KEY,
                alt_staff_id INTEGER NOT NULL REFERENCES staff(alt_id)
            );''',
            '''CREATE TABLE staff (
                id INTEGER PRIMARY KEY,
                alt_id INTEGER UNIQUE,
                manager_id INTEGER REFERENCES managers
            );''',
        ]:
            self.database.execute(sql)
        return SqliteSchema.create_from_conn(self.database.connection)

    def test_alt_staff_manager(self, schema3):
        managers = schema3.tables[0]
        staff = schema3.tables[1]

        rows = [
            (staff, (1, 1, None)),
            (managers, (1, 1)),
            (staff, (2, 2, 1)),
            (staff, (3, 3, 1)),
        ]
        self.database.insert_rows(rows)

        table = {'table': 'staff', 'column': 'id', 'values': 2}
        extraction_model_data = [
            {'subject': [{'tables': [table]}]},
            {'relations': [{'defaults': Relation.DEFAULT_EVERYTHING}]},
        ]
        self.check_launch(schema3, extraction_model_data, rows)
