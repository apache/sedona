check :
	pre-commit run --all-files
.PHONY : check

checkinstall :
	pre-commit install
.PHONY : checkinstall

checkupdate :
	pre-commit autoupdate
.PHONY : checkupdate

docsinstall :
	pip install mkdocs
	pip install mkdocs-jupyter
	pip install mkdocs-material
	pip install mkdocs-macros-plugin
	pip install mkdocs-git-revision-date-localized-plugin
	pip install mike
.PHONY : docsinstall

docsbuild : docsinstall
	mkdocs build
	mike deploy --update-aliases latest-snapshot -b website -p
	mike serve
.PHONY : docsbuild
