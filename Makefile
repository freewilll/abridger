#!/usr/bin/make

PYTHON=python

all: clean python

tests:
	@echo do stuff
	PYTHONPATH=./lib py.test -vs

pep8:
	@echo "#############################################"
	@echo "# Running PEP8 Compliance Tests"
	@echo "#############################################"
	-pep8 -r

pyflakes:
	pyflakes lib/ansible/*.py lib/ansible/*/*.py bin/*

clean:
	@echo "Cleaning up distutils stuff"
	rm -rf build
	rm -rf dist
	rm -rf lib/ansible.egg-info/
	@echo "Cleaning up byte compiled python stuff"
	find . -type f -regex ".*\.py[co]$$" -delete
	@echo "Cleaning up editor backup files"
	find . -type f -not -path ./test/units/inventory_test_data/group_vars/noparse/all.yml~ \( -name "*~" -or -name "#*" \) -delete
	find . -type f \( -name "*.swp" \) -delete
	@echo "Cleaning up output from test runs"
	rm -rf test/test_data
	@echo "Cleaning up authors file"
	rm -f AUTHORS.TXT
	find . -type f -name '*.pyc' -delete

clean-tox:
	rm -rf .tox

python:
	$(PYTHON) setup.py build
