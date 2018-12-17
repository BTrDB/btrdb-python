# Shell to use with Make
SHELL := /bin/bash

# Set important Paths
PROJECT := btrdb
LOCALPATH := $(CURDIR)/$(PROJECT)

# Export targets not associated with files
.PHONY: test coverage pip clean publish uml build deploy install

# Clean build files
clean:
	find . -name "*.pyc" -print0 | xargs -0 rm -rf
	find . -name "__pycache__" -print0 | xargs -0 rm -rf
	find . -name "*-failed-diff.png" -print0 | xargs -0 rm -rf
	-rm -rf htmlcov
	-rm -rf .coverage
	-rm -rf build
	-rm -rf dist
	-rm -rf $(PROJECT).egg-info
	-rm -rf .eggs
	-rm -rf site
	-rm -rf classes_$(PROJECT).png
	-rm -rf packages_$(PROJECT).png
	-rm -rf docs/_build

# Targets for testing
test:
	# python setup.py test
	pytest tests/

# Publish to gh-pages
publish:
	git subtree push --prefix=deploy origin gh-pages

# Autogenerate GRPC/PB files
grpc:
	python -m grpc_tools.protoc -Ibtrdb4/grpcinterface --python_out=btrdb4 --grpc_python_out=btrdb4 btrdb4/grpcinterface/btrdb.proto
	python -m grpc_tools.protoc -Ibtrdb/grpcinterface --python_out=btrdb/grpcinterface --grpc_python_out=btrdb/grpcinterface btrdb/grpcinterface/btrdb.proto
	@echo
	@echo **Please edit the 'btrdb_pb2_grpc.py' files to fix the import statements**

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
