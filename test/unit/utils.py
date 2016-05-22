import tempfile
import yaml


def make_temp_file(contents, dir=None):
    temp = tempfile.NamedTemporaryFile(mode='wt', suffix='.yaml',
                                       delete=False, dir=dir)
    temp.write(yaml.dump(contents, default_flow_style=False))
    temp.close()
    return temp.name
