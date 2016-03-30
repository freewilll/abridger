import os.path
import yaml


def walk(node, process_include):
    if isinstance(node, list):
        new_list = []
        for i, v in enumerate(node):
            if isinstance(node[i], dict) and len(node[i].keys()) == 1 and \
                    node[i].keys()[0] == 'include':
                for new_node in process_include(node[i].values()[0]):
                    new_list.append(new_node)
            else:
                new_list.append(
                    walk(node[i], process_include))
        node[:] = new_list
    elif isinstance(node, dict):
        for k, v in node.items():
            walk(node[k], process_include)

    return node


def _load(filename, include_paths):
    full_filename = None
    for include_path in include_paths:
        full_filename = os.path.join(include_path, filename)
        if os.path.isfile(full_filename):
            break

    if full_filename is None:
        raise Exception('Unable to locate "%s" in include paths %s' %
                        filename, ','.join(sorted(include_paths)))

    data = yaml.load(open(full_filename))
    if not isinstance(data, list):
        raise Exception('The root data in "%s" must be a sequence' %
                        full_filename)

    def process_include(filename_or_list):
        if isinstance(filename_or_list, list):
            processed_includes = []
            for filename in filename_or_list:
                processed_includes.extend(_load(filename, include_paths))
            return processed_includes
        else:
            return _load(filename_or_list, include_paths)

    walk(data, process_include)
    return data


def load(path):
    '''Load a config file, processing includes along the way.'''

    if not os.path.isfile(path):
        raise Exception('No such file: "%s"' % path)
    include_paths = [os.path.dirname(path)]

    return _load(os.path.basename(path), include_paths)
