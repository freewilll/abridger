import tempfile
import yaml


def make_temp_yaml_file(contents, dir=None):
    temp = tempfile.NamedTemporaryFile(mode='wt', suffix='.yaml', dir=dir)
    temp.write(yaml.dump(contents, default_flow_style=False))
    temp.seek(0)
    temp.flush()
    return temp
