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

.PHONY: check checkinstall checkupdate install docsinstall docsbuild clean test

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

install: checkinstall

docsinstall:
	@echo "Installing documentation dependencies..."
	@command -v uv >/dev/null 2>&1 || (echo 'uv not found, install via: curl -LsSf https://astral.sh/uv/install.sh | sh' && exit 1)
	uv sync --group docs

docsbuild: docsinstall
	@echo "Building documentation..."
	uv run mkdocs build
	uv run mike deploy --update-aliases latest-snapshot -b website -p
	uv run mike serve

clean:
	@echo "Cleaning up generated files... (TODO)"
	rm -rf __pycache__
	rm -rf .mypy_cache
	rm -rf .pytest_cache

run-docs:
	docker build -f docker/docs/Dockerfile -t mkdocs-sedona .
	docker run --rm -it -p 8000:8000 -v ${PWD}:/docs mkdocs-sedona
