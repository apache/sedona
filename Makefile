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

SHELL := /bin/bash

# --- Project Configuration ---
PROJECT_NAME := apache-sedona
VENV_DIR := .venv
REQUIREMENTS_DEV := requirements-dev.txt
REQUIREMENTS_DOCS := requirements-docs.txt
PYTHON_ANIMATION_SCRIPT = .github/workflows/scripts/sedona_animation.py

# --- Directories ---
DIST_DIR := dist
BUILD_DIR := build
DOCS_BUILD_DIR := site
PYCACHE_DIR := __pycache__
MYPY_CACHE_DIR := .mypy_cache
PYTEST_CACHE_DIR := .pytest_cache

# --- Python & Virtual Environment Commands ---
PYTHON := $(shell command -v python3 || command -v python)
VENV_PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_PYTHON) -m pip
MKDOCS_CMD := $(VENV_PYTHON) -m mkdocs
MIKE_CMD := $(VENV_PYTHON) -m mike
PRE_COMMIT_CMD := $(VENV_PYTHON) -m pre_commit # Explicit pre-commit command

# --- Docker Configuration ---
DOCKER_IMAGE_NAME := mkdocs-sedona
DOCKER_CONTAINER_NAME := sedona-docs-server
DOCKER_FILE_DOCS := docker/docs/Dockerfile

# --- PHONY Targets ---
.PHONY: all help \
       venv deactivate \
       install install-dev install-docs \
       check checkinstall checkupdate \
       docsbuild docsserve docsdeploy \
       test \
       clean distclean \
       docker-docs-build docker-docs-run docker-docs-stop \
       logo play_sedona_animation

# --- Default Target ---
all: install-dev docsbuild

# --- Help Target ---
help:
	@printf "Makefile for $(PROJECT_NAME)\n"
	@printf "\n"
	@printf "Usage:\n"
	@printf "  make <target>\n"
	@printf "\n"
	@printf "General:\n"
	@printf "  all               : Installs development dependencies and builds documentation (default).\n"
	@printf "  help              : Show this help message.\n"
	@printf "\n"
	@printf "Virtual Environment:\n"
	@printf "  venv              : Create and prepare a Python virtual environment.\n"
	@printf "  deactivate        : Informational target about deactivating the virtual environment.\n"
	@printf "\n"
	@printf "Installation:\n"
	@printf "  install           : Installs core project dependencies (currently aliases install-dev).\n"
	@printf "  install-dev       : Installs development dependencies from $(REQUIREMENTS_DEV).\n"
	@printf "  install-docs      : Installs documentation dependencies from $(REQUIREMENTS_DOCS).\n"
	@printf "\n"
	@printf "Pre-Commit Checks:\n"
	@printf "  check             : Run pre-commit checks on all files.\n"
	@printf "  checkinstall      : Install pre-commit and set up hooks.\n"
	@printf "  checkupdate       : Update pre-commit hooks.\n"
	@printf "\n"
	@printf "Documentation:\n"
	@printf "  docsbuild         : Build the static documentation.\n"
	@printf "  docsserve         : Serve the documentation locally (usually http://127.0.0.1:8000).\n"
	@printf "  docsdeploy        : Build and deploy documentation versions using mike.\n"
	@printf "\n"
	@printf "Testing:\n"
	@printf "  test              : Run project tests (Placeholder - implement your test command here).\n"
	@printf "\n"
	@printf "Cleanup:\n"
	@printf "  clean             : Remove generated build artifacts and caches.\n"
	@printf "  distclean         : Remove all generated files, including virtual environment.\n"
	@printf "\n"
	@printf "Docker for Documentation:\n"
	@printf "  docker-docs-build : Build the Docker image for documentation.\n"
	@printf "  docker-docs-run   : Run the documentation server in a Docker container.\n"
	@printf "  docker-docs-stop  : Stop the running documentation Docker container.\n"
	@printf "\n"
	@printf "Fun:\n"
	@printf "  logo              : Display an animated Apache Sedona logo in the terminal.\n"
	@printf "  play_sedona_animation : Alias for logo.\n"

# --- Virtual Environment Management ---
venv:
	@printf "Setting up Python virtual environment at $(VENV_DIR)...\n"
	@$(SHELL) -c "\
	if [ ! -d \"$(VENV_DIR)\" ]; then \
	   $(PYTHON) -m venv $(VENV_DIR); \
	   echo \"Virtual environment created.\"; \
	else \
	   echo \"Virtual environment already exists.\"; \
	fi && \
	echo \"Ensuring pip, setuptools, and wheel are up-to-date in the venv...\"; \
	$(PIP) install --upgrade pip setuptools wheel"
	@printf "Virtual environment ready.\n"

deactivate:
	@printf "To deactivate the virtual environment, run 'deactivate' in your shell.\n"
	@printf "This target is for informational purposes only and does not deactivate it directly.\n"

# --- Installation Targets ---
install: install-dev
	@printf "Placeholder for core project dependencies if they differ from dev. Currently aliases install-dev.\n"

install-dev: venv
	@printf "Installing development dependencies from $(REQUIREMENTS_DEV)...\n"
	@$(SHELL) -c "\
	if [ -f \"$(REQUIREMENTS_DEV)\" ]; then \
	   $(PIP) install -r $(REQUIREMENTS_DEV); \
	   echo \"Development dependencies installed.\"; \
	else \
	   echo \"Error: $(REQUIREMENTS_DEV) not found. Skipping development dependency installation.\" >&2; \
	   exit 1; \
	fi"

install-docs: venv
	@printf "Installing documentation dependencies from $(REQUIREMENTS_DOCS)...\n"
	@$(SHELL) -c "\
	if [ -f \"$(REQUIREMENTS_DOCS)\" ]; then \
	   $(PIP) install -r $(REQUIREMENTS_DOCS); \
	   echo \"Documentation dependencies installed.\"; \
	else \
	   echo \"Error: $(REQUIREMENTS_DOCS) not found. Skipping documentation dependency installation.\" >&2; \
	   exit 1; \
	fi"

# --- Pre-Commit Checks ---
checkinstall: venv
	@printf "Ensuring pre-commit is installed and hooks are set up...\n"
	@$(SHELL) -c "\
	if ! $(PRE_COMMIT_CMD) --version >/dev/null 2>&1; then \
	   echo \"Installing pre-commit...\"; \
	   $(PIP) install pre-commit; \
	fi && \
	$(PRE_COMMIT_CMD) install"
	@printf "Pre-commit installed and hooks are set.\n"

check: checkinstall
	@printf "Running pre-commit checks on all files...\n"
	@$(PRE_COMMIT_CMD) run --all-files
	@printf "Pre-commit checks completed.\n"

checkupdate: checkinstall
	@printf "Updating pre-commit hooks...\n"
	@$(PRE_COMMIT_CMD) autoupdate
	@printf "Pre-commit hooks updated.\n"

# --- Documentation Targets ---
docsbuild: install-docs
	@printf "Building documentation into $(DOCS_BUILD_DIR)...\n"
	@$(MKDOCS_CMD) build --clean
	@printf "Documentation build complete.\n"

docsserve: install-docs
	@printf "Serving documentation locally (Press Ctrl+C to stop)...\n"
	@$(MKDOCS_CMD) serve

docsdeploy: install-docs
	@printf "Deploying documentation versions with Mike...\n"
	@$(MIKE_CMD) deploy --update-aliases latest-snapshot -b website -p
	@printf "Documentation deployed.\n"

# --- Testing Target ---
test: install-dev
	@printf "Running project tests... (TODO: Implement your test command here, e.g., pytest)\n"
	@# Example: $(VENV_PYTHON) -m pytest
	@printf "Tests completed.\n"

# --- Cleanup Targets ---
clean:
	@printf "Cleaning up generated files and caches...\n"
	@rm -rf $(PYCACHE_DIR) $(MYPY_CACHE_DIR) $(PYTEST_CACHE_DIR)
	@rm -rf $(BUILD_DIR) $(DIST_DIR)
	@rm -rf $(DOCS_BUILD_DIR)
	@printf "Cleanup complete.\n"

distclean: clean
	@printf "Performing a deep clean: removing virtual environment and all generated files.\n"
	@rm -rf $(VENV_DIR)
	@printf "Deep clean complete.\n"

# --- Docker for Documentation ---
docker-docs-build:
	@printf "Building Docker image '$(DOCKER_IMAGE_NAME)' for documentation...\n"
	@docker build -f $(DOCKER_FILE_DOCS) -t $(DOCKER_IMAGE_NAME) .
	@printf "Docker image built.\n"

docker-docs-run: docker-docs-build
	@printf "Running documentation server in Docker container '$(DOCKER_CONTAINER_NAME)' on port 8000...\n"
	@printf "Access at http://localhost:8000\n"
	@docker run --rm -it -p 8000:8000 --name $(DOCKER_CONTAINER_NAME) -v "${PWD}:/docs" $(DOCKER_IMAGE_NAME) $(MKDOCS_CMD) serve --dev-addr 0.0.0.0:8000

docker-docs-stop:
	@printf "Stopping Docker container '$(DOCKER_CONTAINER_NAME)'...\n"
	@docker stop $(DOCKER_CONTAINER_NAME) >/dev/null 2>&1 || true
	@printf "Docker container stopped (if it was running).\n"

# --- Fun Targets ---
logo: play_sedona_animation

play_sedona_animation:
	@printf "Starting Apache Sedona terminal animation...\n"
	@printf "Press Ctrl+C to stop.\n"
	$(PYTHON) $(PYTHON_ANIMATION_SCRIPT)
	@printf "Animation finished.\n"
