# Shell to use with Make
SHELL := /bin/bash

# Set important Paths
PROJECT := btrdb
LOCALPATH := $(CURDIR)/$(PROJECT)


# Sphinx configuration
SPHINXOPTS    	=
SPHINXBUILD   	= sphinx-build
SPHINXBUILDDIR  = docs/build
SPHINXSOURCEDIR = docs/source

# Export targets not associated with files
.PHONY: test coverage pip clean publish uml build deploy install

# Clean build files
clean:
	find . -name "*.pyc" -print0 | xargs -0 rm -rf
	find . -name "__pycache__" -print0 | xargs -0 rm -rf
	find . -name ".DS_Store" -print0 | xargs -0 rm -rf
	-rm -rf docs/build
	-rm -rf htmlcov
	-rm -rf .pytest_cache
	-rm -rf .coverage
	-rm -rf build
	-rm -rf dist
	-rm -rf $(PROJECT).egg-info
	-rm -rf .eggs
	-rm -rf site
	-rm -rf docs/_build
	-rm -rf platform-builds meta.yaml

# Targets for testing
test:
	python setup.py test

# Publish to gh-pages
publish:
	git subtree push --prefix=deploy origin gh-pages

# Autogenerate GRPC/PB files
grpc:
	@echo Generating files:
	python -m grpc_tools.protoc -Ibtrdb/grpcinterface --python_out=btrdb/grpcinterface --grpc_python_out=btrdb/grpcinterface btrdb/grpcinterface/btrdb.proto
	@echo
	@echo Fixing import statements:
	sed -i'.bak' 's/btrdb_pb2 as btrdb__pb2/btrdb.grpcinterface.btrdb_pb2 as btrdb__pb2/' btrdb/grpcinterface/btrdb_pb2_grpc.py


# Build the universal wheel and source distribution
build:
	python setup.py sdist bdist_wheel

# Install the package from source
install:
	python setup.py install

# Deploy to PyPI
deploy:
	python setup.py register
	twine upload dist/*


html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) $(SPHINXSOURCEDIR) $(SPHINXBUILDDIR)
	@echo
	@echo "Build finished. The HTML pages are in $(SPHINXBUILDDIR)/html."
