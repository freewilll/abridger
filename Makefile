#!/usr/bin/make

PYTHON=python

all: python .tox/venv docs

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
	rm -rf htmlcov
	rm -rf lib/abridger.egg-info
	rm -rf docsite/_build/*
	rm -rf docsite/_static/*
	find . -type f -regex ".*\.py[co]$$" -delete
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete

python:
	$(PYTHON) setup.py build

.tox/venv:
	tox -e venv -v

coverage:
	py.test -vs --cov-report=term-missing --cov=abridger --cov=bin

docs: .tox/venv
	. .tox/venv/bin/activate && $(MAKE) -C docsite html
