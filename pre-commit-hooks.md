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
| identity | N/A | check you have set your git identity | N/A |
| check-hooks-apply | N/A | check that all the hooks apply to the repository | N/A |
| [doctoc](https://github.com/thlorenz/doctoc.git) | N/A | automatically keeps your table of contents up to date | v2.2.0 |
| create-pre-commit-docs | python | creates a Markdown file with information on the pre-commit hooks | N/A |
| prettier | node | format files with prettier | N/A |
| maven-spotless-apply | system | automatically formats Java and Scala code using mvn spotless:apply. | N/A |
| check-zip-file-is-not-committed | fail | Zip files are not allowed in the repository | N/A |
| check-makefiles-tabs | system | ensures that Makefiles are indented with tabs | N/A |
| [chmod](https://github.com/Lucas-C/pre-commit-hooks) | N/A | manual hook to be run by macOS or Linux users for a full repository clean up | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | add license for all Batch files | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all C files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all header files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all Java files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all Markdown files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all Makefiles that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all R files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all Scala files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all Shell files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all TOML files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all YAML files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all Python files that don't have a license header | v1.5.5 |
| [insert-license](https://github.com/Lucas-C/pre-commit-hooks) | N/A | automatically adds a licence header to all other files that don't have a license header | v1.5.5 |
| [pyupgrade](https://github.com/asottile/pyupgrade) | N/A | a tool (and pre-commit hook) to automatically upgrade syntax for newer versions of the language | v3.21.0 |
| [black-jupyter](https://github.com/psf/black-pre-commit-mirror) | N/A | format Python files and Jupyter Notebooks with black | 25.9.0 |
| [clang-format](https://github.com/pre-commit/mirrors-clang-format) | N/A | format C files with clang-format | v20.1.7 |
| [bandit](https://github.com/PyCQA/bandit) | N/A | check Python code for security issues | 1.8.6 |
| [codespell](https://github.com/codespell-project/codespell) | N/A | check spelling with codespell | v2.4.1 |
| [gitleaks](https://github.com/gitleaks/gitleaks) | N/A | check for secrets with gitleaks | v8.28.0 |
| [rst-backticks](https://github.com/pre-commit/pygrep-hooks) | N/A | detect common mistake of using single backticks when writing rst | v1.10.0 |
| [rst-directive-colons](https://github.com/pre-commit/pygrep-hooks) | N/A | detect mistake of rst directive not ending with double colon or space before the double colon | v1.10.0 |
| [rst-inline-touching-normal](https://github.com/pre-commit/pygrep-hooks) | N/A | detect mistake of inline code touching normal text in rst | v1.10.0 |
| [check-ast](https://github.com/pre-commit/pre-commit-hooks) | N/A | check Python files for syntax errors | v6.0.0 |
| [check-builtin-literals](https://github.com/pre-commit/pre-commit-hooks) | N/A | check Python files for proper use of built-in literals | v6.0.0 |
| [check-case-conflict](https://github.com/pre-commit/pre-commit-hooks) | N/A | check for case conflicts in file names | v6.0.0 |
| [check-docstring-first](https://github.com/pre-commit/pre-commit-hooks) | N/A | check that docstrings are at the start of functions | v6.0.0 |
| [check-executables-have-shebangs](https://github.com/pre-commit/pre-commit-hooks) | N/A | check that executable scripts have shebang lines | v6.0.0 |
| [check-illegal-windows-names](https://github.com/pre-commit/pre-commit-hooks) | N/A | check for Windows-illegal file names | v6.0.0 |
| [check-json](https://github.com/pre-commit/pre-commit-hooks) | N/A | check JSON files for syntax errors | v6.0.0 |
| [check-merge-conflict](https://github.com/pre-commit/pre-commit-hooks) | N/A | check for merge conflict markers | v6.0.0 |
| [check-shebang-scripts-are-executable](https://github.com/pre-commit/pre-commit-hooks) | N/A | check that scripts with shebangs are executable | v6.0.0 |
| [check-toml](https://github.com/pre-commit/pre-commit-hooks) | N/A | check TOML files for syntax errors | v6.0.0 |
| [check-vcs-permalinks](https://github.com/pre-commit/pre-commit-hooks) | N/A | ensures that links to vcs websites are permalinks | v6.0.0 |
| [check-xml](https://github.com/pre-commit/pre-commit-hooks) | N/A | attempts to load all xml files to verify syntax | v6.0.0 |
| [check-yaml](https://github.com/pre-commit/pre-commit-hooks) | N/A | attempts to load all yaml files to verify syntax | v6.0.0 |
| [debug-statements](https://github.com/pre-commit/pre-commit-hooks) | N/A | check for debugger imports and py37+ `breakpoint()` calls in python source. | v6.0.0 |
| [destroyed-symlinks](https://github.com/pre-commit/pre-commit-hooks) | N/A | detects symlinks which are changed to regular files with a content of a path which that symlink was pointing to | v6.0.0 |
| [detect-aws-credentials](https://github.com/pre-commit/pre-commit-hooks) | N/A | checks for the existence of AWS secrets that you have set up with the AWS CLI | v6.0.0 |
| [detect-private-key](https://github.com/pre-commit/pre-commit-hooks) | N/A | checks for the existence of private keys | v6.0.0 |
| [end-of-file-fixer](https://github.com/pre-commit/pre-commit-hooks) | N/A | makes sure files end in a newline and only a newline | v6.0.0 |
| [file-contents-sorter](https://github.com/pre-commit/pre-commit-hooks) | N/A | sort the lines in specified files (defaults to alphabetical) | v6.0.0 |
| [fix-byte-order-marker](https://github.com/pre-commit/pre-commit-hooks) | N/A | removes UTF-8 byte order marker | v6.0.0 |
| [forbid-submodules](https://github.com/pre-commit/pre-commit-hooks) | N/A | forbids any submodules in the repository | v6.0.0 |
| [mixed-line-ending](https://github.com/pre-commit/pre-commit-hooks) | N/A | replaces or checks mixed line ending | v6.0.0 |
| [name-tests-test](https://github.com/pre-commit/pre-commit-hooks) | N/A | verifies that test files are named correctly | v6.0.0 |
| [requirements-txt-fixer](https://github.com/pre-commit/pre-commit-hooks) | N/A | sorts entries in the Python requirements files and removes incorrect entry for pkg-resources==0.0.0 | v6.0.0 |
| [trailing-whitespace](https://github.com/pre-commit/pre-commit-hooks) | N/A | trims trailing whitespace | v6.0.0 |
| [markdownlint](https://github.com/igorshubovych/markdownlint-cli) | N/A | check Markdown files with markdownlint | v0.45.0 |
| [shellcheck](https://github.com/shellcheck-py/shellcheck-py) | N/A | check Shell scripts with shellcheck | v0.11.0.1 |
| [yamllint](https://github.com/adrienverge/yamllint) | N/A | check YAML files with yamllint | v1.37.1 |
| [oxipng](https://github.com/oxipng/oxipng) | N/A | check PNG files with oxipng | v9.1.5 |
| [blacken-docs](https://github.com/adamchainz/blacken-docs) | N/A | run `black` on python code blocks in documentation files | 1.20.0 |
