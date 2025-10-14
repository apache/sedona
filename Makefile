# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

PYTHON := $(shell command -v python || command -v python3 || echo python)
PIP := $(PYTHON) -m pip
MKDOCS := mkdocs
MIKE := mike

.PHONY: check checkinstall checkupdate install docs docs-full clean test

check:
	@echo "Running pre-commit checks..."
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		echo "Error: pre-commit is not installed. Run 'make checkinstall' first."; \
		exit 1; \
	fi
	pre-commit run --all-files

checkinstall:
	@echo "Installing pre-commit..."
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		$(PIP) install pre-commit; \
	fi
	pre-commit install

checkupdate: checkinstall
	@echo "Updating pre-commit hooks..."
	pre-commit autoupdate

install:
	@echo "(Deprecated shim) Use 'uv sync --group docs --group dev' instead of 'make install'."
	@command -v uv >/dev/null 2>&1 || (echo 'uv not found, install via: curl -LsSf https://astral.sh/uv/install.sh | sh' && exit 1)
	uv sync --group docs --group dev

docs:
	@echo "Building MkDocs site (dependencies via uv)"
	@command -v uv >/dev/null 2>&1 || (echo 'uv not found, install via: curl -LsSf https://astral.sh/uv/install.sh | sh' && exit 1)
	uv sync --group docs
	uv run mkdocs serve

docs-full:
	@echo "Building full documentation (MkDocs + Sphinx Python API)"
	@command -v uv >/dev/null 2>&1 || (echo 'uv not found, install via: curl -LsSf https://astral.sh/uv/install.sh | sh' && exit 1)
	uv sync --group docs
	SPARK_VERSION=$$(mvn help:evaluate -Dexpression=spark.version -q -DforceStdout); \
	uv pip install pyspark==$$SPARK_VERSION; \
	uv pip install -e python/.[all]; \
	uv run bash -c 'cd python/sedona/doc && make clean && make html'; \
	mkdir -p docs/api/pydocs; \
	cp -r python/sedona/doc/_build/html/* docs/api/pydocs/; \
	uv run mkdocs build

clean:
	@echo "Cleaning up generated files... (TODO)"
	rm -rf __pycache__
	rm -rf .mypy_cache
	rm -rf .pytest_cache

run-docs:
	docker build -f docker/docs/Dockerfile -t mkdocs-sedona .
	docker run --rm -it -p 8000:8000 -v ${PWD}:/docs mkdocs-sedona
