# Plan: Modernize Documentation Build Dependencies (Issue #2365)

Goal: Consolidate documentation and dev tooling Python dependencies into pyproject.toml to enable uv-based workflows (e.g. `uv run mkdocs build`) and remove the need for requirements-dev.txt / requirements-docs.txt.

Scope (explicitly in scope):
- Python-level dependencies currently listed in requirements-dev.txt and requirements-docs.txt
- Optional grouping of doc build extras (e.g. `.[docs]`)
- Update CI (docs.yml) to use uv for installing doc dependencies (still keeping Maven/Node/R steps unchanged)
- Provide local developer instructions for building docs with uv
- Maintain reproducibility / pinning strategy consistent with project direction (respecting that core project already moved to pyproject + uv)

Out of scope (for this PR / iteration):
- Changing Node / gulp build process for docs-overrides
- Changing R pkgdown build workflow
- Migrating Java / Scala build (Maven) steps
- Introducing lock file commits unless agreed (decision point captured below)
- Refactoring Sphinx-based Python API doc generation beyond dependency sourcing

Current state summary:
- requirements-dev.txt: pre-commit
- requirements-docs.txt: blacken-docs, mike, mkdocs, mkdocs-git-revision-date-localized-plugin, mkdocs-jupyter, mkdocs-macros-plugin, mkdocs-material, pymdown-extensions
- pyproject.toml currently contains only tooling configs (bandit, codespell) and no [project] metadata (assuming Python packaging is handled inside python/ subproject, not at repo root)
- python/ subproject already modernized (not altering here)
- GitHub Action docs.yml installs docs deps with pip -r requirements-docs.txt then does custom Sphinx + editable install of python subproject.

Design decisions / proposals:
1. Top-level pyproject.toml vs dedicated docs/pyproject.toml:
   - Choose top-level augmentation to avoid multiple Python project roots and allow `uv run` from repo root.
   - Add a minimalist [project] table only if required by uv for dependency groups. uv supports dependency-groups without publishing if [project] metadata exists. We'll add name = "sedona-build-meta", version = "0" (or 0.0.0) and mark as non-publish guidance in comment.
2. Represent doc/dev deps via dependency-groups (PEP 735 emerging) or extras style:
   - uv today supports [tool.uv.dependency-group] (stable) enabling `uv sync --group docs`.
   - Provide both an extra (project.optional-dependencies.docs) for familiarity and a dependency-group mapping to same set (small duplication) OR rely solely on dependency-group. Simplicity: implement both to ease adoption.
3. Pre-commit belongs to a "dev" or "lint" group (not required in runtime). Add group dev.
4. Remove requirements-dev.txt and requirements-docs.txt in a follow-up PR once CI is updated (single atomic change to prevent breakage).
5. Pinning strategy:
   - Use compatible release specifiers (e.g. mkdocs-material>=9,<10) only if project has known compatibility constraints; otherwise leave unpinned initially and rely on uv lock file for determinism if we decide to commit one.
   - Decision point: whether to commit uv.lock (or uv's generated lock file). Recommendation: YES for reproducibility of docs site builds; treat like package-lock.json. Add to .gitignore if rejected.
6. Sphinx-related Python API doc build currently installs: sphinx, sphinx_rtd_theme, pyspark==$SPARK_VERSION, and editable `python` package with extras. We can:
   - Add sphinx and sphinx_rtd_theme to docs group (these are part of doc generation).
   - pyspark must stay version-dynamic based on Maven spark.version; keep the dynamic install logic in workflow (cannot hardcode because build matrix may vary). Not adding pyspark pin in docs group.
7. Workflow changes (docs.yml):
   - Add step to install uv (curl -LsSf https://astral.sh/uv/install.sh | sh)
   - Replace `pip install -r requirements-docs.txt` with `uv sync --group docs --group dev` (if both needed) or `uv sync --all-groups`.
   - Add `uv run pre-commit` (if pre-commit hooks needed in CI; currently they run in separate workflow so maybe skip). Keep minimal.
   - For Sphinx build steps: replace `pip install sphinx sphinx_rtd_theme ...` with adding those to dependency group (except pyspark) then `uv run pip install pyspark==$SPARK_VERSION` or `uv add --dev pyspark==$SPARK_VERSION` ephemeral. Simpler: keep dedicated pip install for pyspark only after uv sync.
   - Editable install of python subproject: use `uv pip install -e python/.[all]` or include in pyproject optional deps? Likely keep as-is but using uv: `uv pip install -e python/.[all]` (ensures same resolver).
   - Run mkdocs via `uv run mkdocs build`.
8. Local dev instructions (README section):
   - Install uv
   - `uv sync --group docs` (and optionally dev) then `uv run mkdocs serve`.
   - Explain dynamic pyspark requirement only if building Python API docs locally; provide snippet to derive Spark version from Maven.
9. Backwards compatibility:
   - For transition PR we can keep requirements files but note they are deprecated and kept temporarily. Or remove and update docs simultaneously (preferred for clarity).

Implementation Steps (ordered):
A. Augment pyproject.toml:
   - Add [project] table (name, version, description, readme, license = {text = "Apache-2.0"}, requires-python >=3.9 maybe) purely for dependency management.
   - Add optional-dependencies.docs list.
   - Add dependency groups in [tool.uv.dependency-groups].
B. Populate docs/deps sets:
   Core doc deps:
     - mkdocs
     - mkdocs-material
     - mkdocs-git-revision-date-localized-plugin
     - mkdocs-jupyter
     - mkdocs-macros-plugin
     - pymdown-extensions
     - mike
     - blacken-docs
     - sphinx
     - sphinx_rtd_theme
   Dev tooling group:
     - pre-commit
   (Future: maybe codespell, bandit if we later migrate them; currently configured but not installed. Decide later.)
C. Update docs.yml workflow:
   - Insert uv installation before Python deps.
   - Remove pip install -r requirements-docs.txt line.
   - Add uv sync + pyspark targeted install + editable sedona python.
   - Replace `mkdocs build` invocation with `uv run mkdocs build`.
D. Remove requirements-dev.txt and requirements-docs.txt (or mark deprecated). (Will do in same PR if no external automation depends on them.)
E. Add ISSUE_2365_PLAN.md (this file) and possibly README snippet referencing new workflow.
F. Optional: add uv lock file and commit (await maintainer decision). Provide note in plan.

Risk & Mitigations:
- Risk: Other automation (outside GitHub Actions) still referencing requirements files. Mitigation: communicate in release notes / add stub files with comment for one release cycle if needed.
- Risk: Without lock file, doc builds may change unexpectedly. Mitigation: commit lock or pin critical packages (mkdocs-material major changes). Recommendation: commit lock file.
- Risk: Adding [project] metadata at repo root may confuse packaging tools. Mitigation: Add comment clarifying not published; choose name clearly internal (e.g., "sedona-monorepo-meta"). Do not include classifiers or dependencies that overlap with python subproject packaging.

Decisions (reviewer feedback applied):
1. Commit uv lock file: YES. Treat uv.lock like package-lock.json for reproducible doc builds.
2. Remove requirements-*.txt: YES remove atomically in same PR (no deprecation stubs) since workflow updated simultaneously.
3. Minimum Python version: Align with python subproject (currently requires-python >=3.8). Use >=3.8 in root pyproject for consistency unless future deprecation plan emerges.
4. bandit/codespell under dependency groups: DEFER. Pre-commit manages its own envs; keep scope narrow to docs/dev replacement.

Post-merge follow-ups (if approved):
- Update contributor docs / website to reflect new doc build instructions.
- Optionally add a Makefile target (e.g., make docs) invoking uv run mkdocs build for convenience.

Example updated workflow snippet (illustrative only):
```
- name: Install uv
  run: curl -LsSf https://astral.sh/uv/install.sh | sh
- name: Sync doc environment
  run: uv sync --group docs --group dev
- name: Install pyspark matching Maven Spark version
  run: |
    SPARK_VERSION=$(mvn help:evaluate -Dexpression=spark.version -q -DforceStdout)
    uv pip install pyspark==$SPARK_VERSION
- name: Install Sedona Python package (editable)
  run: uv pip install -e python/.[all]
- name: Build Sphinx Python API docs
  run: |
    cd python/sedona/doc && make clean && make html
    mkdir -p docs/api/pydocs
    cp -r python/sedona/doc/_build/html/* docs/api/pydocs/
- name: Build MkDocs site
  run: uv run mkdocs build
```

Dependency group definitions draft:
```
[tool.uv.dependency-groups]
  docs = [
    "mkdocs",
    "mkdocs-material",
    "mkdocs-git-revision-date-localized-plugin",
    "mkdocs-jupyter",
    "mkdocs-macros-plugin",
    "pymdown-extensions",
    "mike",
    "blacken-docs",
    "sphinx",
    "sphinx_rtd_theme",
  ]
  dev = ["pre-commit"]
```

Timeline / Effort:
- Single PR with changes A-D (approx <150 LOC diff) pending decisions.

Please review decisions & open questions; once aligned I will implement.
