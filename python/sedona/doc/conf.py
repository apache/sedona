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
    "sphinx.ext.intersphinx",  # Link to other projects' documentation
    "sphinx_rtd_theme",  # Read the Docs theme
]

# Mock imports to handle NumPy 2.0 compatibility issues with PySpark and missing dependencies
# These are needed even in CI because of NumPy 2.0 incompatibility in PySpark
_base_mock_imports = [
    "pyspark.pandas",
    "pyspark.pandas.indexes",
    "pyspark.pandas.indexes.base",
    "pyspark.pandas.series",
    "pyspark.pandas.strings",
    "pystac",
    "sedona.spark.raster.raster_serde",
    "sedona.spark.raster.sedona_raster",
]

# Use the same mocking for both CI and local development
autodoc_mock_imports = _base_mock_imports + [
    "keplergl",
    "pydeck",
    "rasterio",
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
    # Suppress docstring formatting warnings
    "docutils",
    # Suppress specific autodoc import errors that occur due to IndexError
    "autodoc.import_object",
]

# Don't treat warnings as errors to avoid IndexError issues
warning_is_error = False
# Be less strict about references to avoid import errors
nitpicky = False

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "private-members": False,
    "special-members": "__init__",
    "show-inheritance": True,
    "ignore-module-all": False,
}

# Configure autodoc to be more forgiving with import errors
autodoc_inherit_docstrings = True
autodoc_preserve_defaults = True


# Add error handling for problematic imports in CI
def skip_member(app, what, name, obj, skip, options):
    """Skip problematic members that cause IndexError during documentation build.

    This is a Sphinx autodoc-skip-member event handler that filters out members
    known to cause import issues during documentation generation.

    Parameters
    ----------
    app : sphinx.application.Sphinx
        The Sphinx application instance.
    what : str
        The type of the object being documented. Can be one of:
        'module', 'class', 'exception', 'function', 'method', 'attribute'.
    name : str
        The fully qualified name of the object being documented.
    obj : Any
        The actual Python object being documented. May be None if the object
        couldn't be imported.
    skip : bool
        Whether the member was already marked to be skipped by previous handlers
        or Sphinx's default behavior.
    options : dict
        The autodoc options for this object (e.g., :members:, :undoc-members:).

    Returns
    -------
    bool
        True if the member should be skipped, False if it should be documented.
        If skip is already True, this function preserves that decision.
    """
    # Skip members that are known to cause import issues
    problematic_patterns = ["raster_serde", "sedona_raster", "shapely1", "shapely2"]
    if any(pattern in name for pattern in problematic_patterns):
        return True
    return skip


def setup(app):
    """Configure Sphinx app with error handling."""
    app.connect("autodoc-skip-member", skip_member)


# Intersphinx mapping to external documentation
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "shapely": ("https://shapely.readthedocs.io/en/stable", None),
    "geopandas": ("https://geopandas.org/en/stable", None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python", None),
}

# Type aliases for common PySpark types that might not resolve properly
autodoc_type_aliases = {
    "DataFrame": "pyspark.sql.DataFrame",
    "SparkSession": "pyspark.sql.SparkSession",
    "StructType": "pyspark.sql.types.StructType",
    "StructField": "pyspark.sql.types.StructField",
}

# Suppress warnings for known unresolvable references
nitpick_ignore = [
    ("py:class", "pyspark.sql.dataframe.DataFrame"),
    ("py:class", "pyspark.sql.session.SparkSession"),
    ("py:class", "pyspark.sql.types.StructType"),
    ("py:class", "pyspark.sql.types.StructField"),
    ("py:class", "pyspark.rdd.RDD"),
]

# -- Options for HTML output -------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_title = f"{project} Documentation"

# Custom CSS to match Apache Sedona branding
html_css_files = [
    "custom.css",
]

html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 4,
    "style_external_links": False,
    "titles_only": False,
    "logo_only": False,
    "prev_next_buttons_location": "bottom",
    "vcs_pageview_mode": "",
    # Custom colors to match Sedona theme
    "style_nav_header_background": "#ff5722",
}

# Apache Sedona Branding
html_logo = None  # We'll use CSS for logo styling
html_favicon = None

# Additional context for templates
html_context = {
    "display_github": False,
}
