# Install all dependencies if they are not installed
# Pandoc needs to be installed: brew install pandoc
list.of.packages <- c("sparklyr", "roxygen2", "tools")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages, repos = "http://cran.us.r-project.org")

# Generate Rd files in man/
roxygen2::roxygenize()

# Script to convert .Rd and .Rmd files from man/ and vignettes to docs/*.md for use by MkDocs
library("tools")

# Create two dir if not exist
dir.create(file.path("../docs/api/", "rdocs"), showWarnings = FALSE)

# Convert Rd files to markdown

files = dir("man/")
for(file in files){
  infile = file.path("man/", file)
  outfile = file.path("../docs/api/rdocs", gsub(".Rd", ".html", file))
  Rd2HTML(infile, outfile, package = "apache.sedona", stages = c("install", "render"))
}

