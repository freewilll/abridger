#!/usr/bin/make

PYTHON=python

all: clean python

tests:
	PYTHONPATH=./lib py.test -vs

pep8:
	@echo "#############################################"
	@echo "# Running PEP8 Compliance Tests"
	@echo "#############################################"
	-pep8 -r

clean:
	@echo "Cleaning up distutils stuff"
	rm -rf build
	rm -rf dist
	rm -rf .tox
	find . -type f -regex ".*\.py[co]$$" -delete
	find . -type f -name '*.pyc' -delete

clean-tox:
	rm -rf .tox

python:
	$(PYTHON) setup.py build
