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
from __future__ import annotations

from typing import Any, Callable, Optional, Union

from pyspark.sql import Column

import pandas as pd
import geopandas as gpd
import pyspark.pandas as pspd

from sedona.geopandas.base import GeoFrame
from sedona.geopandas._typing import GeoFrameLike, Label
from pyspark.pandas._typing import Axis, Dtype, Scalar
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas import Series as PandasOnSparkSeries


class GeoDataFrame(GeoFrame, pspd.DataFrame):
    """
    A class representing a GeoDataFrame, inheriting from GeoFrame and pyspark.pandas.DataFrame.
    """

    def __getitem__(self, key: Any) -> Any:
        """
        Get item from GeoDataFrame by key.

        Parameters
        ----------
        key : str, list, slice, ndarray or Series
            - If key is a string, returns a Series for that column
            - If key is a list of strings, returns a new GeoDataFrame with selected columns
            - If key is a slice or array, returns rows in the GeoDataFrame

        Returns
        -------
        Any
            Series, GeoDataFrame, or other objects depending on the key type.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> data = {'geometry': [Point(0, 0), Point(1, 1)], 'value': [1, 2]}
        >>> gdf = GeoDataFrame(data)
        >>> gdf['value']
        0    1
        1    2
        Name: value, dtype: int64
        """
        from sedona.geopandas import GeoSeries

        # Handle column access by name
        if isinstance(key, str):
            # Access column directly from the spark DataFrame
            column_name = key

            # Check if column exists
            if column_name not in self.columns:
                raise KeyError(f"Column '{column_name}' does not exist")

            # Get column data from spark_frame
            spark_df = self._internal.spark_frame.select(column_name)
            pandas_df = spark_df.toPandas()

            # Check if this is a geometry column
            field = next(
                (f for f in self._internal.spark_frame.schema.fields if f.name == key),
                None,
            )

            if field and (
                field.dataType.typeName() == "geometrytype"
                or field.dataType.typeName() == "binary"
            ):
                # Return as GeoSeries for geometry columns
                return GeoSeries(pandas_df[column_name])
            else:
                # Return as regular pandas Series for non-geometry columns
                from pyspark.pandas import Series

                return Series(pandas_df[column_name])

        # Handle list of column names
        elif isinstance(key, list) and all(isinstance(k, str) for k in key):
            # Check if all columns exist
            missing_cols = [k for k in key if k not in self.columns]
            if missing_cols:
                raise KeyError(f"Columns {missing_cols} do not exist")

            # Select columns from the spark DataFrame
            spark_df = self._internal.spark_frame.select(*key)
            pandas_df = spark_df.toPandas()

            # Return as GeoDataFrame
            return GeoDataFrame(pandas_df)

        # Handle row selection via slice or boolean indexing
        else:
            # For now, convert to pandas first for row-based operations
            # This could be optimized later for better performance
            pandas_df = self._internal.spark_frame.toPandas()
            selected_rows = pandas_df[key]
            return GeoDataFrame(selected_rows)

    def __init__(
        self,
        data=None,
        index=None,
        columns=None,
        dtype=None,
        copy=False,
        geometry: Any | None = None,
        crs: Any | None = None,
        **kwargs,
    ):
        assert data is not None

        self._anchor: GeoDataFrame
        self._col_label: Label

        from sedona.geopandas import GeoSeries

        if isinstance(
            data, (GeoDataFrame, GeoSeries, PandasOnSparkSeries, PandasOnSparkDataFrame)
        ):
            assert dtype is None
            assert not copy

            self._anchor = data
            self._col_label = index
        else:
            if isinstance(data, pd.DataFrame):
                assert index is None
                assert dtype is None
                assert not copy
                df = data
            else:
                df = pd.DataFrame(
                    data=data,
                    index=index,
                    dtype=dtype,
                    copy=copy,
                )
            gdf = gpd.GeoDataFrame(df)
            # convert each geometry column to wkb type
            for col in gdf.columns:
                if isinstance(gdf[col], gpd.GeoSeries):
                    gdf[col] = gdf[col].apply(lambda geom: geom.wkb)
            pdf = pd.DataFrame(gdf)
            # initialize the parent class pyspark Dataframe with the pandas Series
            super().__init__(
                data=pdf,
                index=index,
                dtype=dtype,
                copy=copy,
            )

    def _reduce_for_geostat_function(
        self,
        sfun: Callable[["GeoSeries"], Column],
        name: str,
        axis: Optional[Axis] = None,
        numeric_only: bool = True,
        skipna: bool = True,
        **kwargs: Any,
    ) -> Union["GeoSeries", Scalar]:
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def dtypes(self) -> Union[gpd.GeoSeries, pd.Series, Dtype]:
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def to_geopandas(self) -> Union[gpd.GeoDataFrame, pd.Series]:
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def _to_geopandas(self) -> Union[gpd.GeoDataFrame, pd.Series]:
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def geoindex(self) -> "GeoIndex":
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def copy(self, deep=False):
        """
        Make a copy of this GeoDataFrame object.

        Parameters:
        - deep: bool, default False
            If True, a deep copy of the data is made. Otherwise, a shallow copy is made.

        Returns:
        - GeoDataFrame: A copy of this GeoDataFrame object.

        Examples:
        >>> from shapely.geometry import Point
        >>> import geopandas as gpd
        >>> from sedona.geopandas import GeoDataFrame

        >>> gdf = GeoDataFrame([{"geometry": Point(1, 1), "value1": 2, "value2": 3}])
        >>> gdf_copy = gdf.copy()
        >>> print(gdf_copy)
           geometry  value1  value2
        0  POINT (1 1)       2       3
        """
        if deep:
            return GeoDataFrame(
                self._anchor.copy(), dtype=self.dtypes, index=self._col_label
            )
        else:
            return self

    @property
    def area(self) -> "GeoDataFrame":
        """
        Returns a GeoDataFrame containing the area of each geometry expressed in the units of the CRS.

        Returns
        -------
        GeoDataFrame
            A GeoDataFrame with the areas of the geometries.

        Examples
        --------
        >>> from shapely.geometry import Polygon
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> data = {
        ...     'geometry': [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]), Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        ...     'value': [1, 2]
        ... }
        >>> gdf = GeoDataFrame(data)
        >>> gdf.area
           geometry_area  value
        0           1.0      1
        1           4.0      2
        """
        # Create a list of all column expressions for the new dataframe
        select_expressions = []

        # Process geometry columns to calculate areas
        for field in self._internal.spark_frame.schema.fields:
            col_name = field.name

            # Skip index column to avoid duplication
            if col_name == "__index_level_0__" or col_name == "__natural_order__":
                continue

            if (
                field.dataType.typeName() == "geometrytype"
                or field.dataType.typeName() == "binary"
            ):
                # Calculate the area for each geometry column
                if field.dataType.typeName() == "binary":
                    area_expr = (
                        f"ST_Area(ST_GeomFromWKB(`{col_name}`)) as {col_name}_area"
                    )
                else:
                    area_expr = f"ST_Area(`{col_name}`) as {col_name}_area"
                select_expressions.append(area_expr)
            else:
                # Keep non-geometry columns as they are
                select_expressions.append(f"`{col_name}`")

        # Execute the query to get all data in one go
        result_df = self._internal.spark_frame.selectExpr(*select_expressions)

        # Convert to pandas DataFrame
        pandas_df = result_df.toPandas()

        # Create a new GeoDataFrame with the result
        # Note: This avoids the need to manipulate the index columns separately
        return GeoDataFrame(pandas_df)

    @property
    def crs(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @crs.setter
    def crs(self, value):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def geom_type(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def type(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def length(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_valid(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def is_valid_reason(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_empty(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def count_coordinates(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def count_geometries(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def count_interior_rings(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_simple(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_ring(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_ccw(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def is_closed(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def has_z(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def get_precision(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def get_geometry(self, index):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def boundary(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def centroid(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def concave_hull(self, ratio=0.0, allow_holes=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def convex_hull(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def delaunay_triangles(self, tolerance=0.0, only_edges=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def voronoi_polygons(self, tolerance=0.0, extend_to=None, only_edges=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def envelope(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_rotated_rectangle(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def exterior(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def extract_unique_points(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def offset_curve(self, distance, quad_segs=8, join_style="round", mitre_limit=5.0):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def interiors(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def remove_repeated_points(self, tolerance=0.0):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def set_precision(self, grid_size, mode="valid_output"):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def representative_point(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_bounding_circle(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_bounding_radius(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def minimum_clearance(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def normalize(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def make_valid(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def reverse(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def segmentize(self, max_segment_length):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def transform(self, transformation, include_z=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def force_2d(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def force_3d(self, z=0):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def line_merge(self, directed=False):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    @property
    def unary_union(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def union_all(self, method="unary", grid_size=None):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def intersection_all(self):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def contains(self, other, align=None):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def contains_properly(self, other, align=None):
        # Implementation of the abstract method
        raise NotImplementedError("This method is not implemented yet.")

    def buffer(
        self,
        distance,
        resolution=16,
        cap_style="round",
        join_style="round",
        mitre_limit=5.0,
        single_sided=False,
        **kwargs,
    ) -> "GeoDataFrame":
        """
        Returns a GeoDataFrame with all geometries buffered by the specified distance.

        Parameters
        ----------
        distance : float
            The distance to buffer by. Negative distances will create inward buffers.
        resolution : int, default 16
            The number of segments used to approximate curves.
        cap_style : str, default "round"
            The style of the buffer cap. One of 'round', 'flat', 'square'.
        join_style : str, default "round"
            The style of the buffer join. One of 'round', 'mitre', 'bevel'.
        mitre_limit : float, default 5.0
            The mitre limit ratio for joins when join_style='mitre'.
        single_sided : bool, default False
            Whether to create a single-sided buffer.

        Returns
        -------
        GeoDataFrame
            A new GeoDataFrame with buffered geometries.

        Examples
        --------
        >>> from shapely.geometry import Point
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> data = {
        ...     'geometry': [Point(0, 0), Point(1, 1)],
        ...     'value': [1, 2]
        ... }
        >>> gdf = GeoDataFrame(data)
        >>> buffered = gdf.buffer(0.5)
        """
        # Create a list of all column expressions for the new dataframe
        select_expressions = []

        # Process each field in the schema
        for field in self._internal.spark_frame.schema.fields:
            col_name = field.name

            # Skip index and order columns
            if col_name == "__index_level_0__" or col_name == "__natural_order__":
                continue

            # Apply buffer to geometry columns
            if (
                field.dataType.typeName() == "geometrytype"
                or field.dataType.typeName() == "binary"
            ):

                if field.dataType.typeName() == "binary":
                    # For binary geometry columns (WKB)
                    buffer_expr = f"ST_Buffer(ST_GeomFromWKB(`{col_name}`), {distance}) as {col_name}"
                else:
                    # For native geometry columns
                    buffer_expr = f"ST_Buffer(`{col_name}`, {distance}) as {col_name}"
                select_expressions.append(buffer_expr)
            else:
                # Keep non-geometry columns as they are
                select_expressions.append(f"`{col_name}`")

        # Execute the query to get all data in one go
        result_df = self._internal.spark_frame.selectExpr(*select_expressions)

        # Convert to pandas DataFrame and create a new GeoDataFrame
        pandas_df = result_df.toPandas()
        return GeoDataFrame(pandas_df)

    def sjoin(
        self,
        other,
        how="inner",
        predicate="intersects",
        lsuffix="left",
        rsuffix="right",
        distance=None,
        on_attribute=None,
        **kwargs,
    ):
        """
        Spatial join of two GeoDataFrames.

        Parameters
        ----------
        other : GeoDataFrame
            The right GeoDataFrame to join with.
        how : str, default 'inner'
            The type of join:
            * 'left': use keys from left_df; retain only left_df geometry column
            * 'right': use keys from right_df; retain only right_df geometry column
            * 'inner': use intersection of keys from both dfs; retain only
              left_df geometry column
        predicate : str, default 'intersects'
            Binary predicate. Valid values: 'intersects', 'contains', 'within', 'dwithin'
        lsuffix : str, default 'left'
            Suffix to apply to overlapping column names (left GeoDataFrame).
        rsuffix : str, default 'right'
            Suffix to apply to overlapping column names (right GeoDataFrame).
        distance : float, optional
            Distance for 'dwithin' predicate. Required if predicate='dwithin'.
        on_attribute : str, list or tuple, optional
            Column name(s) to join on as an additional join restriction.
            These must be found in both DataFrames.

        Returns
        -------
        GeoDataFrame
            A GeoDataFrame with the results of the spatial join.

        Examples
        --------
        >>> from shapely.geometry import Point, Polygon
        >>> from sedona.geopandas import GeoDataFrame
        >>>
        >>> polygons = GeoDataFrame({
        ...     'geometry': [Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])],
        ...     'value': [1]
        ... })
        >>> points = GeoDataFrame({
        ...     'geometry': [Point(0.5, 0.5), Point(2, 2)],
        ...     'value': [1, 2]
        ... })
        >>> joined = points.sjoin(polygons)
        """
        from sedona.geopandas.tools.sjoin import sjoin as sjoin_tool

        return sjoin_tool(
            self,
            other,
            how=how,
            predicate=predicate,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
            distance=distance,
            on_attribute=on_attribute,
            **kwargs,
        )

    def to_parquet(self, path, **kwargs):
        """
        Write the GeoSeries to a GeoParquet file.

        Parameters:
        - path: str
            The file path where the GeoParquet file will be written.
        - kwargs: Any
            Additional arguments to pass to the Sedona DataFrame output function.
        """
        # Use the Spark DataFrame's write method to write to GeoParquet format
        self._internal.spark_frame.write.format("geoparquet").save(path, **kwargs)

    def to_wkt(self, **kwargs):
        """
        Encode all geometry columns in the GeoDataFrame to WKT.

        Parameters
        ----------
        kwargs
            rounding_precision

        Returns
        -------
        DataFrame
            geometry columns are encoded to WKT
        """

        # Create a list of all column expressions for the new dataframe
        select_expressions = []

        # Read the keyword arguments
        rounding_precision = kwargs.get("rounding_precision", None)

        # Process geometry columns to get wkts
        for field in self._internal.spark_frame.schema.fields:
            col_name = field.name
            # Skip index column to avoid duplication
            if col_name == "__index_level_0__" or col_name == "__natural_order__":
                continue

            if field.dataType.typeName() in {"geometrytype", "binary"}:
                # Start with the base expression.
                if field.dataType.typeName() == "binary":
                    base_expr = f"ST_GeomFromWKB(`{col_name}`)"
                else:
                    base_expr = f"`{col_name}`"

                # Chain transformations if the keyword argument is provided.
                if rounding_precision is not None:
                    base_expr = f"ST_ReducePrecision({base_expr}, {rounding_precision})"

                wkt_expr = f"ST_AsEWKT({base_expr}) as {col_name}"
                select_expressions.append(wkt_expr)
            else:
                # Keep non-geometry columns as they are
                select_expressions.append(f"`{col_name}`")

        # Execute the query to get all data in one go
        result_df = self._internal.spark_frame.selectExpr(*select_expressions)

        # Convert to pandas DataFrame
        pandas_df = result_df.toPandas()

        # Create a new GeoDataFrame with the result
        # Note: This avoids the need to manipulate the index columns separately
        return GeoDataFrame(pandas_df)
