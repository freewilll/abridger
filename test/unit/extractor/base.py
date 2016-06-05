import pytest
from pprint import pprint

from abridger.extraction_model import ExtractionModel
from abridger.extractor import Extractor


class TestExtractorBase(object):
    @pytest.fixture(autouse=True)
    def default_fixtures(self, sqlite_conn, sqlite_database):
        self.database = sqlite_database
        self.conn = sqlite_conn

    def check_launch(self, schema, extraction_model_data,
                     expected_data, global_relations=None,
                     expected_fetch_count=None, one_subject=True):
        if global_relations is not None:
            extraction_model_data.append({'relations': global_relations})

        extraction_model = ExtractionModel.load(schema, extraction_model_data)
        extractor = Extractor(self.database, extraction_model).launch()
        expected_data = sorted(expected_data, key=lambda t: t[0].name)

        if extractor.flat_results() != expected_data:
            print()
            print('Got results:')
            pprint(extractor.flat_results())
            print('Expected results:')
            pprint(expected_data)
        assert extractor.flat_results() == expected_data
        if expected_fetch_count is not None:
            assert extractor.fetch_count == expected_fetch_count
        return extractor

    def check_one_subject(self, schema, tables, expected_data,
                          relations=None, global_relations=None,
                          expected_fetch_count=None):
        extraction_model_data = [{'subject': [{'tables': tables}]}]
        if relations is not None:
            extraction_model_data[0]['subject'].append(
                {'relations': relations})

        self.check_launch(schema, extraction_model_data, expected_data,
                          global_relations=global_relations)

    def check_two_subjects(self, schema, tables, expected_data,
                           relations=None, global_relations=None,
                           expected_fetch_count=None):
        extraction_model_data = []
        for t in tables:
            subject = [{'tables': [t]}]
            if relations is not None:
                subject.append({'relations': relations})
            extraction_model_data.append({'subject': subject})
        self.check_launch(schema, extraction_model_data, expected_data,
                          global_relations=global_relations)
