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

.PHONY: check checkinstall checklinks checkupdate install docsinstall docsbuild clean test

check:
	@echo "Running pre-commit checks..."
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		echo "Error: pre-commit is not installed. Run 'make checkinstall' first."; \
		exit 1; \
	fi
	SKIP=lychee pre-commit run --all-files

checkinstall:
	@echo "Installing pre-commit..."
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		$(PIP) install pre-commit; \
	fi
	pre-commit install

checklinks:
	pre-commit run lychee --all-files

checkupdate: checkinstall
	@echo "Updating pre-commit hooks..."
	pre-commit autoupdate

install:
	@echo "Installing dependencies..."
	@if [ -f requirements-dev.txt ]; then \
		$(PIP) install -r requirements-dev.txt; \
	else \
		echo "Error: requirements-dev.txt not found."; \
		exit 1; \
	fi

docsinstall:
	@echo "Installing documentation dependencies..."
	$(PIP) install -r requirements-docs.txt

docsbuild: docsinstall
	@echo "Building documentation..."
	$(MKDOCS) build
	$(MIKE) deploy --update-aliases latest-snapshot -b website -p
	$(MIKE) serve

clean:
	@echo "Cleaning up generated files... (TODO)"
	rm -rf __pycache__
	rm -rf .mypy_cache
	rm -rf .pytest_cache

run-docs:
	docker build -f docker/docs/Dockerfile -t mkdocs-sedona .
	docker run --rm -it -p 8000:8000 -v ${PWD}:/docs mkdocs-sedona
