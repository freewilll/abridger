SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
BUILDDIR      = _build
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

all: clean html

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*

sphinx:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
.PHONY: sphinx

.PHONY: html
html: examples getting_started sphinx
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

examples: examples.rst.j2
	../.tox/venv/bin/python make_examples.py

getting_started: getting_started.rst.j2
	../.tox/venv/bin/python make_getting_started.py
