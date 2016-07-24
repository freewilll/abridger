#!/usr/bin/env python

import os
import sys

sys.path.insert(0, os.path.abspath('lib'))  # noqa
from abridger import __version__, __author__

try:
    from setuptools import setup, find_packages
except ImportError:
    print("Abridger needs setuptools in order to build. Install it using"
          " your package manager (usually python-setuptools) or via pip (pip"
          " install setuptools).")
    sys.exit(1)

setup(
    name='abridger',
    version=__version__,
    description='Abridger database subsetting tool',
    author=__author__,
    author_email='w.angenent@gmail.com',
    url='https://github.com/freewilll/abridger',
    license='MIT',
    install_requires=['setuptools', 'pyyaml', 'six',
                      'jsonschema', 'dj-database-url', 'future'],
    package_dir={'': 'lib'},
    packages=find_packages('lib'),
    data_files=[],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: SQL',
        'Topic :: Database',
        'Topic :: Utilities',
    ],
    keywords=['database', 'subset', 'filter', 'reduce', 'sqlite', 'postgresql',
              'sql'],
    scripts=['bin/abridge-db', 'bin/abridger-dump-relations'],
)
