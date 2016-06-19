import os.path
import pytest
import tempfile
from abridger.config_file_loader import load
from abridger.exc import IncludeError, DataError, FileNotFoundError
from .utils import make_temp_yaml_file


class TestConfigFileLoader(object):
    def setup(self):
        self.temp_dir = tempfile.mkdtemp()

    def make_temp_yaml_file(self, contents, dir=None):
        if dir is None:
            dir = self.temp_dir
        return make_temp_yaml_file(contents, dir=dir)

    def test_load_unknown_file(self):
        with pytest.raises(FileNotFoundError):
            load('foo')

    def test_bad_non_mapping_in_root(self):
        with pytest.raises(DataError):
            tempfile = make_temp_yaml_file({})
            load(tempfile.name)

    def test_include_toplevel_string(self):
        '''Test - include: foo.yaml'''
        filename2 = self.make_temp_yaml_file([{'relations': []}])
        filename1 = self.make_temp_yaml_file([{'include': filename2.name}])
        assert load(filename1.name) == [{'relations': []}]

    def test_include_toplevel_array(self):
        '''Test - include: [foo.yaml, bar.yaml]'''
        filename2 = self.make_temp_yaml_file([{'relations': [{'a': 'b'}]}])
        filename3 = self.make_temp_yaml_file([{'relations': [{'c': 'd'}]}])
        filename1 = self.make_temp_yaml_file([{'include': [filename2.name,
                                                           filename3.name]}])
        assert load(filename1.name) == [
            {'relations': [{'a': 'b'}]},
            {'relations': [{'c': 'd'}]}]

    def test_include_toplevel_subdir(self):
        '''Test -include: foo/bar.yaml'''
        temp_dir = tempfile.mkdtemp(dir=self.temp_dir)
        filename2 = self.make_temp_yaml_file([{'relations': []}], temp_dir)
        short_filename2 = os.path.join(
            os.path.basename(temp_dir),
            os.path.basename(filename2.name))
        filename1 = self.make_temp_yaml_file([{'include': short_filename2}])
        assert load(filename1.name) == [{'relations': []}]

    def test_include_deep(self):
        '''Test: - fetch: [{include: foo.yaml}]'''

        filename2 = self.make_temp_yaml_file([{'relations': []}])
        filename1 = self.make_temp_yaml_file([
            {
                'fetch': [{
                    'include': filename2.name
                }]
            }
        ])
        assert load(filename1.name) == [{'fetch': [{'relations': []}]}]

    def test_include_deep_unknown_file(self):
        '''Test: - fetch: [{include: foo.yaml}]'''

        filename1 = self.make_temp_yaml_file([
            {
                'fetch': [{
                    'include': 'unknown.yaml'
                }]
            }
        ])
        with pytest.raises(IncludeError):
            assert load(filename1.name) == [{'fetch': [{'relations': []}]}]
