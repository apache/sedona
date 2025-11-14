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

import numpy as np
from pyspark.pandas.utils import log_advice
from pyspark.sql import DataFrame as PySparkDataFrame
from shapely.geometry.base import BaseGeometry

from sedona.spark import StructuredAdapter
from sedona.spark.core.enums import IndexType

# Add this near the top of the file, after imports
ALLOWED_PREDICATES = ["intersects", "contains"]


class SpatialIndex:
    """
    A wrapper around Sedona's spatial index functionality.
    """

    def __init__(self, geometry, index_type="strtree", column_name=None):
        """
        Initialize the SpatialIndex with geometry data.

        Parameters
        ----------
        geometry : np.array of Shapely geometries, GeoSeries, or PySparkDataFrame
        index_type : str, default "strtree"
            The type of spatial index to use.
        column_name : str, optional
            The column name to extract geometry from if `geometry` is a PySparkDataFrame.

        Note: query methods (i.e. query, nearest, intersection) have different behaviors depending on how the index is constructed.
        When constructed from a np.array, the query methods return indices like original GeoPandas.
        When constructed from a GeoSeries or PySparkDataFrame, the query methods return geometries.
        """
        from sedona.spark.geopandas import GeoSeries

        if isinstance(geometry, GeoSeries):
            from sedona.spark.geopandas.geoseries import _get_series_col_name

            column_name = _get_series_col_name(geometry)
            geometry = geometry._internal.spark_frame

        if isinstance(geometry, np.ndarray):
            self.geometry = geometry
            self.index_type = index_type
            self._dataframe = None
            self._is_spark = False
            # Build local index for NumPy array.
            self._build_local_index()
        elif isinstance(geometry, PySparkDataFrame):
            if column_name is None:
                raise ValueError(
                    "column_name must be specified when geometry is a PySparkDataFrame"
                )
            self.geometry = geometry[column_name]
            self.index_type = index_type
            self._dataframe = geometry
            self._is_spark = True
            # Build distributed spatial index.
            self._build_spark_index(column_name)
        else:
            raise TypeError(
                "Invalid type for `geometry`. Expected np.array, GeoSeries, or PySparkDataFrame."
            )

    def query(self, geometry: BaseGeometry, predicate: str = None, sort: bool = False):
        """
        Query the spatial index for geometries that intersect the given geometry.

        Parameters
        ----------
        geometry : Shapely geometry
            The geometry to query against the spatial index.

        predicate : str, optional
            Spatial predicate to filter results. Must be either 'intersects' (default) or 'contains'.
        sort : bool, optional, default False
            Whether to sort the results.

        Note: query() has different behaviors depending on how the index is constructed.
        When constructed from a np.array, this method returns indices like original GeoPandas.
        When constructed from a GeoSeries or PySparkDataFrame, this method returns geometries.

        Note: Unlike GeoPandas, Sedona does not support geometry input of type np.array or GeoSeries.
        It is recommended to instead use GeoSeries.intersects directly.

        Returns
        -------
        list
            List of geometries if constructed from a GeoSeries or PySparkDataFrame.
            List of the corresponding indices if constructed from a np.array.
        """

        if not isinstance(geometry, BaseGeometry):
            raise TypeError(
                "Sedona only supports shapely geometries as input to `query`."
            )

        log_advice(
            "`query` returns a local list onto driver's memory. "
            "It should only be used if the resulting collection is expected to be small."
        )

        if self.is_empty:
            return []

        # Validate predicate value
        if predicate is not None and predicate not in ALLOWED_PREDICATES:
            raise ValueError(
                f"Predicate must be either {' or '.join([repr(p) for p in ALLOWED_PREDICATES])}, got '{predicate}'"
            )

        # Default to 'intersects' if not specified.
        if predicate is None:
            predicate = "intersects"

        if self._is_spark:
            # For Spark-based spatial index.
            from sedona.spark.core.spatialOperator import RangeQuery

            # Execute the spatial range query.
            if predicate == "contains":
                result_rdd = RangeQuery.SpatialRangeQuery(
                    self._indexed_rdd, geometry, False, True
                )
            else:  # intersects
                result_rdd = RangeQuery.SpatialRangeQuery(
                    self._indexed_rdd, geometry, True, True
                )

            geo_data_list = result_rdd.collect()
            # No need to keep the userData field, so convert it directly to a list of geometries.
            geoms_list = [row.geom for row in geo_data_list]
            return geoms_list
        else:
            # For local spatial index based on Shapely STRtree.
            if predicate == "contains":
                # STRtree doesn't directly support contains predicate.
                # We need to filter results after querying.
                candidate_indices = self._index.query(geometry)
                results = [
                    i for i in candidate_indices if geometry.contains(self.geometry[i])
                ]
            else:  # intersects
                # Default is intersects.
                results = self._index.query(geometry)

            if sort and results:
                # Sort by distance to the query geometry if requested.
                results = sorted(
                    results, key=lambda i: self.geometry[i].distance(geometry)
                )

            return results

    def nearest(
        self, geometry: BaseGeometry, k: int = 1, return_distance: bool = False
    ):
        """
        Find the nearest geometry in the spatial index.

        Parameters
        ----------
        geometry : Shapely geometry
            The geometry to find the nearest neighbor for.

        k : int, optional, default 1
            Number of nearest neighbors to find.
        return_distance : bool, optional, default False
            Whether to return distances along with indices.

        Note: Unlike GeoPandas, Sedona does not support geometry input of type np.array or GeoSeries.

        Note: nearest() has different behaviors depending on how the index is constructed.
        When constructed from a np.array, this method returns indices like original GeoPandas.
        When constructed from a GeoSeries or PySparkDataFrame, this method returns geometries.

        Returns
        -------
        list or tuple
            List of geometries if constructed from a GeoSeries or PySparkDataFrame.
            List of the corresponding indices if constructed from a np.array.
        """

        if not isinstance(geometry, BaseGeometry):
            raise TypeError(
                "Sedona only supports shapely geometries as input to `nearest`."
            )

        log_advice(
            "`nearest` returns local list of indices of matching geometries onto driver's memory. "
            "It should only be used if the resulting collection is expected to be small."
        )

        if self.is_empty:
            return [] if not return_distance else ([], [])

        if self._is_spark:
            # For Spark-based spatial index.
            from sedona.spark.core.spatialOperator import KNNQuery

            # Execute the KNN query.
            geo_data_list = KNNQuery.SpatialKnnQuery(
                self._indexed_rdd, geometry, k, False
            )

            # No need to keep the userData field, so convert it directly to a list of geometries.
            geoms_list = [row.geom for row in geo_data_list]

            if return_distance:
                # Calculate distances if requested.
                distances = [geom.distance(geometry) for geom in geoms_list]
                return geoms_list, distances
            return geoms_list
        else:
            # For local spatial index based on Shapely STRtree.
            if k > len(self.geometry):
                k = len(self.geometry)

            # Get all geometries and calculate distances.
            distances = np.array([geom.distance(geometry) for geom in self.geometry])

            # Get indices of k nearest neighbors.
            indices = np.argsort(distances)[:k]

            if return_distance:
                return indices.tolist(), distances[indices].tolist()
            return indices.tolist()

    def intersection(self, bounds):
        """
        Find geometries that intersect the given bounding box. Similar to the Geopandas version,
        this is a compatibility wrapper for rtree.index.Index.intersection, use query instead.

        Parameters
        ----------
        bounds : tuple
            Bounding box as (min_x, min_y, max_x, max_y).

        Note: intersection() has different behaviors depending on how the index is constructed.
        When constructed from a np.array, this method returns indices like original GeoPandas.
        When constructed from a GeoSeries or PySparkDataFrame, this method returns geometries.

        Note: Unlike GeoPandas, Sedona does not support geometry input of type np.array or GeoSeries.
        It is recommended to instead use GeoSeries.intersects directly.

        Returns
        -------
        list
            List of geometries if constructed from a GeoSeries or PySparkDataFrame.
            List of the corresponding indices if constructed from a np.array.
        """
        log_advice(
            "`intersection` returns local list of matching geometries onto driver's memory. "
            "It should only be used if the resulting collection is expected to be small."
        )

        if self.is_empty:
            return []

        # Create a polygon from the bounds.
        from shapely.geometry import box

        bbox = box(*bounds)

        if self._is_spark:
            return self.query(bbox, predicate="intersects")
        else:
            # For local spatial index based on Shapely STRtree.
            try:
                # Try using direct bounds query (Shapely >= 1.8).
                return self._index.query(bounds)
            except TypeError:
                # Fall back to querying with a box geometry (older Shapely).
                return self._index.query(bbox)

    @property
    def size(self):
        """
        Get the size of the spatial index.

        Returns
        -------
        int
            Number of geometries in the index.
        """
        if self._is_spark:
            return self._dataframe.count()
        return len(self.geometry)

    @property
    def is_empty(self):
        """
        Check if the spatial index is empty.

        Returns
        -------
        bool
            True if the index is empty, False otherwise.
        """
        return self.size == 0

    def _build_spark_index(self, column_name):
        """
        Build a distributed spatial index on the geometry column of the DataFrame.

        This uses Sedona's built-in indexing functionality.
        """

        # Convert index_type string to Sedona IndexType enum.
        index_type_map = {"strtree": IndexType.RTREE, "quadtree": IndexType.QUADTREE}
        sedona_index_type = index_type_map.get(self.index_type.lower(), IndexType.RTREE)

        # Create a SpatialRDD from the DataFrame.
        spatial_rdd = StructuredAdapter.toSpatialRdd(self._dataframe, column_name)

        # Build spatial index.
        spatial_rdd.buildIndex(sedona_index_type, False)

        # Store the indexed RDD.
        self._indexed_rdd = spatial_rdd

    def _build_local_index(self):
        """
        Build a local spatial index for numpy array of geometries.
        """
        from shapely.strtree import STRtree

        if len(self.geometry) > 0:
            if self.index_type.lower() == "strtree":
                self._index = STRtree(self.geometry)
            else:
                raise ValueError(
                    f"Unsupported index type: {self.index_type}. Only 'strtree' is supported for local indexing."
                )
