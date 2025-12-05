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

.PHONY: check check-from-ref check-install check-last check-stage clean docsbuild docsinstall install run-docs \
		sync-no-dev update update-deps

check: install ## run all pre-commit checks
	prek run --all-files

check-from-ref: install ## will run prek checks for all changes since you branched off
	prek run --from-ref main

check-install: ## checks for a uv installation
	@command -v uv >/dev/null 2>&1 || (echo 'uv not found, install via: curl -LsSf https://astral.sh/uv/install.sh | sh' && exit 1)

check-last: install ## will run pre-commit checks on last commit only
	prek --last-commit

check-stage: install ## runs check on currently staged changes
	prek

clean:
	@echo "Cleaning up generated files... (TODO)"
	rm -rf __pycache__
	rm -rf .mypy_cache
	rm -rf .pytest_cache

docsbuild: docsinstall
	@echo "Building documentation..."
	uv run mkdocs build
	uv run mike deploy --update-aliases latest-snapshot -b website -p
	uv run mike serve

docsinstall: check-install
	@echo "Installing documentation dependencies..."
	uv sync --group docs

install: check-install ## Sync all dependencies (including development) using uv
	uv sync --all-groups

run-docs:
	docker build -f docker/docs/Dockerfile -t mkdocs-sedona .
	docker run --rm -it -p 8000:8000 -v ${PWD}:/docs mkdocs-sedona

sync-no-dev: check-install ## Sync non-development dependencies using uv
	uv sync --no-dev

update: install
	prek auto-update

update-deps: check-install ## Update pre-commit hooks and dependency locks
	prek auto-update || :
	uv lock --upgrade
	uv sync --all-groups
