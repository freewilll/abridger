import os.path
import pytest
import tempfile
from minime.config_file_loader import load
from utils import make_temp_file


class TestConfigFileLoader(object):
    def setup(self):
        self.temp_dir = tempfile.mkdtemp()

    def make_temp_file(self, contents, dir=None):
        if dir is None:
            dir = self.temp_dir
        return make_temp_file(contents, dir=dir)

    def test_load_unknown_file(self):
        with pytest.raises(Exception) as e:
            load('foo')
        assert 'No such file' in str(e)

    def test_bad_non_mapping_in_root(self):
        with pytest.raises(Exception) as e:
            load(make_temp_file({}))
        assert 'must be a sequence' in str(e)

    def test_include_toplevel_string(self):
        '''Test - include: foo.yaml'''
        filename2 = self.make_temp_file([{'relations': []}])
        filename1 = self.make_temp_file([{'include': filename2}])
        assert load(filename1) == [{'relations': []}]

    def test_include_toplevel_array(self):
        '''Test - include: [foo.yaml, bar.yaml]'''
        filename2 = self.make_temp_file([{'relations': [{'a': 'b'}]}])
        filename3 = self.make_temp_file([{'relations': [{'c': 'd'}]}])
        filename1 = self.make_temp_file([{'include': [filename2, filename3]}])
        assert load(filename1) == [
            {'relations': [{'a': 'b'}]},
            {'relations': [{'c': 'd'}]}]

    def test_include_toplevel_subdir(self):
        '''Test -include: foo/bar.yaml'''
        temp_dir = tempfile.mkdtemp(dir=self.temp_dir)
        filename2 = self.make_temp_file([{'relations': []}], temp_dir)
        short_filename2 = os.path.join(
            os.path.basename(temp_dir),
            os.path.basename(filename2))
        filename1 = self.make_temp_file([{'include': short_filename2}])
        assert load(filename1) == [{'relations': []}]

    def test_include_deep(self):
        '''Test: - fetch: [{include: foo.yaml}]'''

        filename2 = self.make_temp_file([{'relations': []}])
        filename1 = self.make_temp_file([
            {
                'fetch': [{
                    'include': filename2
                }]
            }
        ])
        assert load(filename1) == [{'fetch': [{'relations': []}]}]
