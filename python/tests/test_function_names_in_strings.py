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
"""Guard: Sedona's own Python code must not name a Sedona function in a string.

With ``spark.sedona.function.prefix`` set, only prefixed function names exist in
SQL. A function name inside a string sent to Spark would break. This test needs
no SparkSession and no JVM: it only parses the source files with ``ast``.
"""

import ast
import os
import re

SOURCE_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sedona", "spark")
)

FUNCTION_NAME_PATTERN = re.compile(
    r"\b(?:ST|RS)_[A-Za-z0-9]\w*"
    r"|\b(?:GeometryType|ExpandAddress|ParseAddress|Barrier)\s*\("
    r"|__sedona_internal_\w+",
    re.IGNORECASE,
)

# Values returned by ST_GeometryType: data, not function names, so the prefix does not change them.
GEOMETRY_TYPE_VALUES = frozenset(
    {
        "ST_Point",
        "ST_LineString",
        "ST_Polygon",
        "ST_MultiPoint",
        "ST_MultiLineString",
        "ST_MultiPolygon",
        "ST_GeometryCollection",
    }
)

# Whole files that may hold function-name strings. Paths are relative to python/sedona/spark.
ALLOWED_FILES = {
    # Each wrapper passes its function name to call_sedona_function, which applies the prefix.
    "sql/st_functions.py",
    "sql/st_constructors.py",
    "sql/st_predicates.py",
    "sql/st_aggregates.py",
    "sql/dataframe_api.py",
}

# File -> exact literals allowed in it. Each reason says what the string is used for.
ALLOWED_LITERALS = {
    "geopandas/geoseries.py": {
        # Name passed to call_sedona_function_by_name, which applies the prefix (two call sites).
        "__sedona_internal_coverage_invalid_edges_for_target",
    },
    "geopandas/tools/sjoin_nearest.py": {
        # Name passed to call_sedona_function_by_name for the nearest-neighbour join condition.
        "ST_KNN",
    },
    "raster_utils/SedonaUtils.py": {
        # Name passed to call_sedona_function_by_name to render a raster column as an image.
        "RS_AsImage",
    },
    "register/java_libs.py": {
        # Fully qualified names of the Scala st_* expression holder objects, loaded through py4j
        # as Java classes; they are class paths, not SQL function names.
        "org.apache.spark.sql.sedona_sql.expressions.st_constructors",
        "org.apache.spark.sql.sedona_sql.expressions.st_functions",
        "org.apache.spark.sql.sedona_sql.expressions.st_predicates",
        "org.apache.spark.sql.sedona_sql.expressions.st_aggregates",
    },
}

FAILURE_HINT = (
    "Use sedona.spark.sql st_functions / st_constructors / st_predicates, "
    "or call_sedona_function_by_name, instead of a function name in a string."
)


def _python_files():
    found = []
    for directory, _, names in os.walk(SOURCE_ROOT):
        for name in names:
            if name.endswith(".py"):
                found.append(os.path.join(directory, name))
    return sorted(found)


def _relative(path):
    return os.path.relpath(path, SOURCE_ROOT).replace(os.sep, "/")


def _docstring_nodes(tree):
    nodes = set()
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                nodes.add(id(body[0].value))
    return nodes


def _hits(path):
    """Return (line, literal) for each string constant in the file that names a function."""
    with open(path, encoding="utf-8") as source:
        tree = ast.parse(source.read(), filename=path)
    docstrings = _docstring_nodes(tree)
    result = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        if id(node) in docstrings or node.value in GEOMETRY_TYPE_VALUES:
            continue
        if FUNCTION_NAME_PATTERN.search(node.value):
            result.append((node.lineno, node.value))
    return result


def test_no_function_names_in_strings():
    files = _python_files()
    assert os.path.isdir(SOURCE_ROOT), f"Source directory not found: {SOURCE_ROOT}"
    assert (
        len(files) > 50
    ), f"Expected more than 50 .py files under {SOURCE_ROOT}, found {len(files)}"

    violations = []
    seen_literals = {}
    files_with_hits = set()
    for path in files:
        rel = _relative(path)
        if rel in ALLOWED_FILES:
            if _hits(path):
                files_with_hits.add(rel)
            continue
        allowed = ALLOWED_LITERALS.get(rel, set())
        for line, literal in _hits(path):
            if literal in allowed:
                seen_literals.setdefault(rel, set()).add(literal)
            else:
                violations.append(f"{rel}:{line}: {literal!r}")

    stale = [
        f"file {rel}" for rel in sorted(ALLOWED_FILES) if rel not in files_with_hits
    ]
    for rel, literals in ALLOWED_LITERALS.items():
        for literal in sorted(literals - seen_literals.get(rel, set())):
            stale.append(f"{rel}: {literal!r}")

    problems = []
    if violations:
        problems.append("Function names in strings:\n  " + "\n  ".join(violations))
    if stale:
        problems.append("Stale allow-list entries:\n  " + "\n  ".join(stale))
    assert not problems, "\n".join(problems) + "\n" + FAILURE_HINT
