#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

# Install all dependencies if they are not installed
# Pandoc needs to be installed: brew install pandoc
list.of.packages <- c("sparklyr", "roxygen2", "tools")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages, repos = "http://cran.us.r-project.org")

# Generate Rd files in man/
roxygen2::roxygenize("R")

# Script to convert .Rd and .Rmd files from man/ and vignettes to docs/*.md for use by MkDocs
library("tools")

# Create two dir if not exist
dir.create(file.path("docs/api/", "rdocs"), showWarnings = FALSE)

# Convert Rd files to markdown

files = dir("R/man/")
for(file in files){
  infile = file.path("R/man/", file)
  outfile = file.path("docs/api/rdocs", gsub(".Rd", ".html", file))
  Rd2HTML(infile, outfile, package = "apache.sedona", stages = c("install", "render"))
}

