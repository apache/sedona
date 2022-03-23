#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
