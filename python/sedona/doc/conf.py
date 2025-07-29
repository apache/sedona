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

# Configuration file for the Sphinx documentation builder.

import os
import sys

# Resolve the relative path to the `sedona` package
sedona_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, sedona_path)

# -- Project information -----------------------------------------------------
project = "Apache Sedona Python"
copyright = "2025, Apache Software Foundation"
author = "Apache Software Foundation"
version = "1.7.2"
release = "1.7.2"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",  # For Google-style or NumPy-style docstrings
    "sphinx.ext.viewcode",  # Adds links to source code
    "sphinx_rtd_theme",  # Read the Docs theme
]

# Mock imports to handle NumPy 2.0 compatibility issues with PySpark and missing dependencies
autodoc_mock_imports = [
    "pyspark.pandas",
    "pyspark.pandas.indexes",
    "pyspark.pandas.indexes.base",
    "pyspark.pandas.series",
    "pyspark.pandas.strings",
    "pystac",
    "sedona.spark.raster.raster_serde",
    "sedona.spark.raster.sedona_raster",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Suppress specific warnings
suppress_warnings = [
    # Suppress warnings about mocked objects (these are intentionally mocked)
    "autodoc.mocked_object",
    # Suppress warnings about multiple targets for cross-references
    # This is expected due to shapely1/shapely2 compatibility layer
    "ref.python",
]

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "private-members": False,
    "special-members": "__init__",
    "show-inheritance": True,
}

# -- Options for HTML output -------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_title = f"{project} Documentation"

html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 4,
    "style_external_links": True,
    "titles_only": False,
}
