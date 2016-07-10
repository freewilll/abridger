SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
BUILDDIR      = _build
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

all: clean html

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*

.PHONY: html
html: examples getting_started
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

examples: examples.rst.j2
	../.tox/venv/bin/python make-examples.py

getting_started: getting_started.rst.j2
	../.tox/venv/bin/python make-getting_started.py
