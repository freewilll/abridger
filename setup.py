#!/usr/bin/env python

import os
import sys

sys.path.insert(0, os.path.abspath('lib'))
from minime import __version__, __author__  # noqa

try:
    from setuptools import setup, find_packages
except ImportError:
    print("Minime needs setuptools in order to build. Install it using"
          " your package manager (usually python-setuptools) or via pip (pip"
          " install setuptools).")
    sys.exit(1)

setup(
    name='minime',
    version=__version__,
    description='Minime database subsetting tool',
    author=__author__,
    author_email='minime@example.com',
    url='http://minime.example.com/',
    license='GPLv3',
    install_requires=['setuptools'],
    package_dir={'': 'lib'},
    packages=find_packages('lib'),
    data_files=[],
)
