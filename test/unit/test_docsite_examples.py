import pytest
import sys

sys.path.append('docsite')  # noqa
from docsite.make_examples import main


# I'm not interested in maintaining the docsite generation scripts
# for python < 3.0
@pytest.mark.skipif("sys.version_info < (3,0)")
class TestDocSiteMakeExamples(object):
    def test_docsite_examples(self):
        main(testing=True)
