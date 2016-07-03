SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
BUILDDIR      = _build
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

all: clean html

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*

.PHONY: html
html: examples
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

examples: examples.rst.j2
	../.tox/venv/bin/python ../bin/tools/make-docsite-examples.py
