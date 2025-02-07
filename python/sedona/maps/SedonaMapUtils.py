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

import json

from sedona.sql.types import GeometryType
from sedona.utils.geoarrow import dataframe_to_arrow
from packaging.version import parse


class SedonaMapUtils:

    @classmethod
    def __convert_to_gdf_or_pdf__(cls, df, rename=True, geometry_col=None):
        """
        Converts a SedonaDataFrame to a GeoPandasDataFrame and also renames geometry column to a standard name of
        'geometry'
        However, if no geometry column is found even after traversing schema, returns a Pandas Dataframe
        :param df: SedonaDataFrame to convert
        :param geometry_col: [Optional]
        :return: GeoPandas Dataframe or Pandas Dataframe
        """
        if geometry_col is None:
            geometry_col = SedonaMapUtils.__get_geometry_col__(df)

        # Convert the dataframe to arrow format, then to geopandas dataframe
        # This is faster than converting directly to geopandas dataframe via toPandas
        if (
            geometry_col is None
        ):  # No geometry column found even after searching schema, return Pandas Dataframe
            data_pyarrow = dataframe_to_arrow(df)
            return data_pyarrow.to_pandas()
        try:
            import geopandas as gpd
        except ImportError:
            msg = "GeoPandas is missing. You can install it manually or via apache-sedona[kepler-map] or apache-sedona[pydeck-map]."
            raise ImportError(msg) from None
        # From GeoPandas 1.0.0 onwards, the from_arrow method is available
        if parse(gpd.__version__) >= parse("1.0.0"):
            data_pyarrow = dataframe_to_arrow(df)
            geo_df = gpd.GeoDataFrame.from_arrow(data_pyarrow)
        else:
            geo_df = gpd.GeoDataFrame(df.toPandas(), geometry=geometry_col)
        if geometry_col != "geometry" and rename is True:
            geo_df.rename_geometry("geometry", inplace=True)
        return geo_df

    @classmethod
    def __convert_to_geojson__(cls, df):
        """
        Converts a SedonaDataFrame to GeoJSON
        :param df: SedonaDataFrame to convert
        :return: GeoJSON object
        """
        gdf = SedonaMapUtils.__convert_to_gdf_or_pdf__(df)
        gjson_str = gdf.to_json()
        gjson = json.loads(gjson_str)
        return gjson

    @classmethod
    def __get_geometry_col__(cls, df):
        schema = df.schema
        for field in schema.fields:
            if field.dataType == GeometryType():
                return field.name

    @classmethod
    def __extract_coordinate__(cls, geom, type_list):
        geom_type = geom.geom_type
        if SedonaMapUtils.__is_geom_collection__(geom_type):
            geom = SedonaMapUtils._extract_first_sub_geometry_(geom)
            geom_type = geom.geom_type
        if geom_type not in type_list:
            type_list.append(geom_type)
        if geom_type == "Polygon":
            return geom.exterior.coords[0]
        else:
            return geom.coords[0]

    @classmethod
    def __extract_point_coordinate__(cls, geom):
        if geom.geom_type == "Point":
            return geom.coords[0]

    @classmethod
    def _extract_first_sub_geometry_(cls, geom):
        while SedonaMapUtils.__is_geom_collection__(geom.geom_type):
            geom = geom.geoms[0]
        return geom

    @classmethod
    def __is_geom_collection__(cls, geom_type):
        return (
            geom_type == "MultiPolygon"
            or geom_type == "MultiLineString"
            or geom_type == "MultiPoint"
            or geom_type == "GeometryCollection"
        )
