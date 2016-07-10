import os


def file_path(filename):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), filename))


def read_file(filename):
    return open(file_path(filename)).read()
