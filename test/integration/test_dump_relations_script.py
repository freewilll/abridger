import os
import re
import sys
import tempfile
import yaml

sys.path.insert(0, os.path.abspath('bin'))  # noqa
from dump_relations import main


class TestDumpRelationsScript(object):
    def test_main(self, postgresql):
        # This doesn't test the data itself, just the executable.
        # The script only supports postgresql, so a postgresql database
        # has to be created to test with.
        m = re.match(r'user=(.+) host=(.+) port=(\d+) dbname=(.+)',
                     postgresql.dsn)
        (user, host, port, dbname) = (m.group(1), m.group(2), m.group(3),
                                      m.group(4))
        data = {
            'dbname': dbname,
            'host': host,
            'port': port,
            'user': user,
        }
        temp = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        temp.write(yaml.dump(data, default_flow_style=False))
        temp.close()
        main([temp.name])
