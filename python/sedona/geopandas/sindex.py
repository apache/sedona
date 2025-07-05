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


class SpatialIndex:
    """
    A wrapper around Sedona's spatial index functionality.
    """

    def __init__(self, geometry, index_type="strtree", column_name=None):
        """
        Initialize the SpatialIndex with geometry data.

        Parameters
        ----------
        geometry : np.array of Shapely geometries, or PySparkDataFrame.
        index_type : str, default "strtree"
            The type of spatial index to use.
        column_name : str, optional
            The column name to extract geometry from if `geometry` is a GeoDataFrame.
        """

        if isinstance(geometry, np.ndarray):
            self.geometry = geometry
        elif isinstance(geometry, PySparkDataFrame):
            if column_name is None:
                raise ValueError("column_name must be specified for PySpark DataFrame.")
            if column_name not in geometry.columns:
                raise ValueError(
                    f"Column '{column_name}' does not exist in the DataFrame."
                )
            self.geometry = geometry[column_name].values
        else:
            raise TypeError(
                "Invalid type for `geometry`. Expected np.array, GeoSeries, or GeoDataFrame."
            )

        self.index_type = index_type

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
        # Placeholder for range query using Sedona
        raise NotImplementedError("This method is not implemented yet.")

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
