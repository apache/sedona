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

import os
from typing import Union
import warnings
import pyspark.pandas as ps
from sedona.spark.geopandas import GeoDataFrame
from pyspark.pandas.utils import default_session, scol_for
from pyspark.pandas.internal import SPARK_DEFAULT_INDEX_NAME, NATURAL_ORDER_COLUMN_NAME
from pyspark.pandas.frame import InternalFrame
from pyspark.pandas.utils import validate_mode, log_advice
from pandas.api.types import is_integer_dtype


def _to_file(
    df: GeoDataFrame,
    path: str,
    driver: Union[str, None] = None,
    index: Union[bool, None] = True,
    **kwargs,
):
    """
    Write the ``GeoDataFrame`` to a file.

    Parameters
    ----------
    path : string
        File path or file handle to write to.
    driver : string, default None
        The format driver used to write the file.
        If not specified, it attempts to infer it from the file extension.
        If no extension is specified, Sedona will error.
        Options:
            - "geojson"
            - "geopackage"
            - "geoparquet"
    schema : dict, default None
        Not applicable to Sedona's implementation.
    index : bool, default None
        If True, write index into one or more columns (for MultiIndex).
        Default None writes the index into one or more columns only if
        the index is named, is a MultiIndex, or has a non-integer data
        type. If False, no index is written.
    mode : string, default 'w'
        The write mode, 'w' to overwrite the existing file and 'a' to append.
        'overwrite' and 'append' are equivalent to 'w' and 'a' respectively.
    crs : pyproj.CRS, default None
        If specified, the CRS is passed to Fiona to
        better control how the file is written. If None, GeoPandas
        will determine the CRS based on CRS df attribute.
        The value can be anything accepted
        by :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
        such as an authority string (eg "EPSG:4326") or a WKT string.
    engine : str
        Not applicable to Sedona's implementation.
    metadata : dict[str, str], default None
        Optional metadata to be stored in the file. Keys and values must be
        strings. Supported only for "GPKG" driver. Not supported by Sedona.
    **kwargs :
        Keyword args to be passed to the engine, and can be used to write
        to multi-layer data, store data within archives (zip files), etc.
        In case of the "pyogrio" engine, the keyword arguments are passed to
        `pyogrio.write_dataframe`. In case of the "fiona" engine, the keyword
        arguments are passed to fiona.open`. For more information on possible
        keywords, type: ``import pyogrio; help(pyogrio.write_dataframe)``.

    Examples
    --------

    >>> gdf = GeoDataFrame({"geometry": [Point(0, 0), LineString([(0, 0), (1, 1)])], "int": [1, 2]}
    >>> gdf.to_file(filepath, format="geoparquet")

    With selected drivers you can also append to a file with `mode="a"`:

    >>> gdf.to_file(gdf, driver="geojson", mode="a")

    When the index is of non-integer dtype, index=None (default) is treated as True, writing the index to the file.

    >>> gdf = GeoDataFrame({"geometry": [Point(0, 0)]}, index=["a", "b"])
    >>> gdf.to_file(gdf, driver="geoparquet")
    """

    ext_to_driver = {
        ".parquet": "GeoParquet",
        ".json": "GeoJSON",
        ".geojson": "GeoJSON",
    }

    # Auto detect driver from filename if not provided.
    if driver is None:
        _, extension = os.path.splitext(path)
        if extension not in ext_to_driver:
            raise ValueError(f"Unsupported file extension: {extension}")
        driver = ext_to_driver[extension]

    spark_fmt = driver.lower()

    crs = kwargs.pop("crs", None)
    if crs:
        from pyproj import CRS

        crs = CRS.from_user_input(crs)

    spark_df = df._internal.spark_frame.drop(NATURAL_ORDER_COLUMN_NAME)

    if index is None:
        # Determine if index attribute(s) should be saved to file
        # (only if they are named or are non-integer).
        index = list(df.index.names) != [None] or not is_integer_dtype(df.index.dtype)

    if not index:
        log_advice(
            "If index is not True is not specified for `to_file`, "
            "the existing index is lost when writing to a file."
        )
        spark_df = spark_df.drop(SPARK_DEFAULT_INDEX_NAME)

    if spark_fmt == "geoparquet":
        writer = spark_df.write.format("geoparquet")

    elif spark_fmt == "geojson":
        writer = spark_df.write.format("geojson")

    else:
        raise ValueError(f"Unsupported spark format: {spark_fmt}")

    default_mode = "overwrite"
    mode = validate_mode(kwargs.pop("mode", default_mode))

    writer.mode(mode).save(path, **kwargs)


def read_file(filename: str, format: Union[str, None] = None, **kwargs):
    """
    Alternate constructor to create a ``GeoDataFrame`` from a file.

    Parameters
    ----------
    filename : str
        File path or file handle to read from. If the path is a directory,
        Sedona will read all files in the directory into a dataframe.
    format : str, default None
        The format of the file to read. If None, Sedona will infer the format
        from the file extension. Note, inferring the format from the file extension
        is not supported for directories.
        Options:
            - "shapefile"
            - "geojson"
            - "geopackage"
            - "geoparquet"
    See also
    --------
    GeoDataFrame.to_file : write GeoDataFrame to file
    """

    # We warn the user if they try to use arguments that GeoPandas supports but not Sedona.
    if kwargs:
        warnings.warn(f"The given arguments are not supported in Sedona: {kwargs}")

    spark = default_session()

    # If format is not specified, infer it from the file extension
    if format is None:
        if os.path.isdir(filename):
            raise ValueError(
                f"Inferring the format from the file extension is not supported for directories: {filename}"
            )
        if filename.lower().endswith(".shp"):
            format = "shapefile"
        elif filename.lower().endswith(".json"):
            format = "geojson"
        elif filename.lower().endswith(".parquet"):
            format = "geoparquet"
        elif filename.lower().endswith(".gpkg"):
            format = "geopackage"
        else:
            raise ValueError(f"Unsupported file type: {filename}")
    else:
        format = format.lower()

    if format == "shapefile":
        sdf = spark.read.format("shapefile").load(filename)
        return GeoDataFrame(sdf)
    elif format == "geojson":
        sdf = (
            spark.read.format("geojson")
            .option("multiLine", "true")
            .load(filename)
            .select(
                "geometry", f"properties.*"
            )  # select all non-geometry columns (which are under properties)
        )
        # GeoJSON also has a 'type' field, but we ignore it.

    elif format == "geopackage":
        table_name = kwargs.get("table_name", None)
        if not table_name:
            raise ValueError("table_name is required for geopackage")
        sdf = (
            spark.read.format("geopackage")
            .option("tableName", table_name)
            .load(filename)
        )

    elif format == "geoparquet":
        sdf = spark.read.format("geoparquet").load(filename)

    else:
        raise NotImplementedError(f"Unsupported file type: {filename}")

    index_spark_columns = []

    # If index was retained, we sort by it so the DataFrame has the same order as the original one.
    if SPARK_DEFAULT_INDEX_NAME in sdf.columns:
        sdf = sdf.orderBy(SPARK_DEFAULT_INDEX_NAME)
        index_spark_columns = [scol_for(sdf, SPARK_DEFAULT_INDEX_NAME)]

    internal = InternalFrame(spark_frame=sdf, index_spark_columns=index_spark_columns)
    return GeoDataFrame(ps.DataFrame(internal))


def read_parquet(
    path,
    columns=None,
    storage_options=None,
    bbox=None,
    to_pandas_kwargs=None,
    **kwargs,
):
    """
    Load a Parquet object from the file path, returning a GeoDataFrame.

    * if no geometry columns are read, this will raise a ``ValueError`` - you
      should use the pandas `read_parquet` method instead.

    If 'crs' key is not present in the GeoParquet metadata associated with the
    Parquet object, it will default to "OGC:CRS84" according to the specification.

    Parameters
    ----------
    path : str, path object
    columns : list-like of strings, default=None
        Not currently supported in Sedona
    storage_options : dict, optional
        Not currently supported in Sedona
    bbox : tuple, optional
        Not currently supported in Sedona
    to_pandas_kwargs : dict, optional
        Not currently supported in Sedona

    Returns
    -------
    GeoDataFrame

    Examples
    --------
    from sedona.spark.geopandas import read_parquet
    >>> df = read_parquet("data.parquet")  # doctest: +SKIP

    Specifying columns to read:

    >>> df = read_parquet(
    ...     "data.parquet",
    ... )  # doctest: +SKIP
    """
    if kwargs:
        warnings.warn(f"The given arguments are not supported in Sedona: {kwargs}")

    return read_file(path, format="geoparquet", **kwargs)
