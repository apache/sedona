<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# pre-commit hook documentation

 | Hook ID | Language | Description | Version |
|---|---|---|---|
| identity | N/A | checks to see if your identity is set | N/A |
| check-hooks-apply | N/A | makes sure all hooks apply to the repository | N/A |
| create-pre-commit-docs | python | creates a Markdown file with information on the pre-commit hooks | N/A |
| prettier | node | format files with prettier | N/A |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all .c files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all .h files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all Java files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all Markdown files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all Makefile files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all R files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all Scala files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all TOML files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all YAML files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all Python files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all other files | v1.5.5 |
| [pyupgrade](https://github.com/asottile/pyupgrade) | N/A | N/A | v3.20.0 |
| [black-jupyter](https://github.com/psf/black-pre-commit-mirror) | N/A | format Python files and Jupyter Notebooks with black | 25.1.0 |
| [clang-format](https://github.com/pre-commit/mirrors-clang-format) | N/A | format C files with clang-format | v20.1.7 |
| [bandit](https://github.com/PyCQA/bandit) | N/A | check Python code for security issues | 1.8.6 |
| [codespell](https://github.com/codespell-project/codespell) | N/A | check spelling with codespell | v2.4.1 |
| [gitleaks](https://github.com/gitleaks/gitleaks) | N/A | check for secrets with gitleaks | v8.27.2 |
| [check-ast](https://github.com/pre-commit/pre-commit-hooks) | N/A | check Python files for syntax errors | v5.0.0 |
| [check-builtin-literals](https://github.com/pre-commit/pre-commit-hooks) | N/A | check Python files for proper use of built-in literals | v5.0.0 |
| [check-case-conflict](https://github.com/pre-commit/pre-commit-hooks) | N/A | check for case conflicts in file names | v5.0.0 |
| [check-docstring-first](https://github.com/pre-commit/pre-commit-hooks) | N/A | check that docstrings are at the start of functions | v5.0.0 |
| [check-executables-have-shebangs](https://github.com/pre-commit/pre-commit-hooks) | N/A | check that executable scripts have shebang lines | v5.0.0 |
| [check-illegal-windows-names](https://github.com/pre-commit/pre-commit-hooks) | N/A | check for Windows-illegal file names | v5.0.0 |
| [check-json](https://github.com/pre-commit/pre-commit-hooks) | N/A | check JSON files for syntax errors | v5.0.0 |
| [check-merge-conflict](https://github.com/pre-commit/pre-commit-hooks) | N/A | check for merge conflict markers | v5.0.0 |
| [check-shebang-scripts-are-executable](https://github.com/pre-commit/pre-commit-hooks) | N/A | check that scripts with shebangs are executable | v5.0.0 |
| [check-toml](https://github.com/pre-commit/pre-commit-hooks) | N/A | check TOML files for syntax errors | v5.0.0 |
| [check-vcs-permalinks](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [check-xml](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [check-yaml](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [debug-statements](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [destroyed-symlinks](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [detect-aws-credentials](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [detect-private-key](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [end-of-file-fixer](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [file-contents-sorter](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [fix-byte-order-marker](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [forbid-submodules](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [mixed-line-ending](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [name-tests-test](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [requirements-txt-fixer](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [trailing-whitespace](https://github.com/pre-commit/pre-commit-hooks) | N/A | N/A | v5.0.0 |
| [markdownlint](https://github.com/igorshubovych/markdownlint-cli) | N/A | check Markdown files with markdownlint | v0.45.0 |
| [shellcheck](https://github.com/shellcheck-py/shellcheck-py) | N/A | check Shell scripts with shellcheck | v0.10.0.1 |
| [yamllint](https://github.com/adrienverge/yamllint) | N/A | check YAML files with yamllint | v1.37.1 |
| [oxipng](https://github.com/shssoichiro/oxipng) | N/A | check PNG files with oxipng | v9.1.5 |
