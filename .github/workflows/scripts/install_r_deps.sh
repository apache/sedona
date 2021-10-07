#!/bin/bash

set -efux -o pipefail

TEST_DEPS="testthat"
SEP='"\\s+"'

R_REMOTES_NO_ERRORS_FROM_WARNINGS=true Rscript - <<_RSCRIPT_EOF_
  if (!require(remotes))
    install.packages("remotes")
  remotes::install_deps(pkgdir = "./R", dependencies = c("Imports"), upgrade = "always")

  test_deps <- strsplit("$TEST_DEPS", $SEP)[[1]]
  for (pkg in test_deps)
    if (!require(pkg, character.only = TRUE))
      install.packages(pkg)

  remotes::install_github("sparklyr/sparklyr", ref = "main", upgrade = TRUE)
_RSCRIPT_EOF_
