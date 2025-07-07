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
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import BinaryType

from sedona.spark.utils.adapter import Adapter
from sedona.spark.core.enums import IndexType


class SpatialIndex:
    """
    A wrapper around Sedona's spatial index functionality.
    """

    def __init__(self, geometry, index_type="strtree", column_name=None):
        """
        Initialize the SpatialIndex with geometry data.

        Parameters
        ----------
        geometry : np.array of Shapely geometries, PySparkDataFrame column, or PySparkDataFrame
        index_type : str, default "strtree"
            The type of spatial index to use.
        column_name : str, optional
            The column name to extract geometry from if `geometry` is a PySparkDataFrame.
        """

        if isinstance(geometry, np.ndarray):
            self.geometry = geometry
            self.index_type = index_type
            self._dataframe = None
            self._is_spark = False
            # Build local index for numpy array
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
            # Build distributed spatial index
            self._build_spark_index(column_name)
        else:
            raise TypeError(
                "Invalid type for `geometry`. Expected np.array or PySparkDataFrame."
            )

    def query(self, geometry, predicate=None, sort=False):
        """
        Query the spatial index for geometries that intersect the given geometry.

        Parameters
        ----------
        geometry : Shapely geometry
            The geometry to query against the spatial index.
        predicate : str, optional
            Spatial predicate to filter results (e.g., 'intersects', 'contains').
        sort : bool, optional, default False
            Whether to sort the results.

        Returns
        -------
        list
            List of indices of matching geometries.
        """
        if self.is_empty:
            return []

        if self._is_spark:
            # For Spark-based spatial index
            from sedona.spark.core.spatialOperator import RangeQuery
            from sedona.spark.core.geom.envelope import Envelope

            # Execute the spatial range query
            if predicate == "contains":
                result_rdd = RangeQuery.SpatialRangeQuery(
                    self._indexed_rdd, geometry, True, True
                )
            else:  # Default to intersects
                result_rdd = RangeQuery.SpatialRangeQuery(
                    self._indexed_rdd, geometry, False, True
                )

            # Convert results to indices
            results = list(range(result_rdd.count()))
            return results
        else:
            # For local spatial index based on Shapely STRtree
            if predicate == "contains":
                # STRtree doesn't directly support contains predicate
                # We need to filter results after querying
                candidate_indices = self._index.query(geometry)
                results = [
                    i for i in candidate_indices if geometry.contains(self.geometry[i])
                ]
            else:
                # Default is intersects
                results = self._index.query(geometry)

            if sort and results:
                # Sort by distance to the query geometry if requested
                results = sorted(
                    results, key=lambda i: self.geometry[i].distance(geometry)
                )

            return results

    def nearest(self, geometry, k=1, return_distance=False):
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

        Returns
        -------
        list or tuple
            List of indices of nearest geometries, optionally with distances.
        """
        # Placeholder for KNN query using Sedona
        raise NotImplementedError("This method is not implemented yet.")

    def intersection(self, bounds):
        """
        Find geometries that intersect the given bounding box.

        Parameters
        ----------
        bounds : tuple
            Bounding box as (min_x, min_y, max_x, max_y).

        Returns
        -------
        list
            List of indices of matching geometries.
        """
        raise NotImplementedError("This method is not implemented yet.")

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

        # Convert index_type string to Sedona IndexType enum
        index_type_map = {"strtree": IndexType.RTREE, "quadtree": IndexType.QUADTREE}
        sedona_index_type = index_type_map.get(self.index_type.lower(), IndexType.RTREE)

        # Create a SpatialRDD from the DataFrame
        spatial_rdd = Adapter.toSpatialRdd(self._dataframe, column_name)

        # Build spatial index
        spatial_rdd.buildIndex(sedona_index_type, False)

        # Store the indexed RDD
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
                # Fallback to STRtree if not supported
                self._index = STRtree(self.geometry)
