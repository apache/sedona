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


import geopandas as gpd
import json
from sedona.sql.types import GeometryType


class SedonaMapUtils:

    @classmethod
    def __convert_to_gdf__(cls, df, rename=True, geometry_col=None):
        """
        Converts a SedonaDataFrame to a GeoPandasDataFrame and also renames geometry column to a standard name of
        'geometry' :param df: SedonaDataFrame to convert :param geometry_col: [Optional] :return:
        """
        if geometry_col is None:
            geometry_col = SedonaMapUtils.__get_geometry_col__(df)
        pandas_df = df.toPandas()
        geo_df = gpd.GeoDataFrame(pandas_df, geometry=geometry_col)
        if geometry_col != "geometry" and rename is True:
            geo_df = geo_df.rename(columns={geometry_col: "geometry"})
        return geo_df

    @classmethod
    def __convert_to_geojson__(cls, df):
        """
        Converts a SedonaDataFrame to GeoJSON
        :param df: SedonaDataFrame to convert
        :return: GeoJSON object
        """
        gdf = SedonaMapUtils.__convert_to_gdf__(df)
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
        if geom_type == 'Polygon':
            return geom.exterior.coords[0]
        else:
            return geom.coords[0]

    @classmethod
    def __extract_point_coordinate__(cls, geom):
        if geom.geom_type == 'Point':
            return geom.coords[0]

    @classmethod
    def _extract_first_sub_geometry_(cls, geom):
        while SedonaMapUtils.__is_geom_collection__(geom.geom_type):
            geom = geom.geoms[0]
        return geom

    @classmethod
    def __is_geom_collection__(cls, geom_type):
        return geom_type == 'MultiPolygon' or geom_type == 'MultiLineString' or geom_type == 'MultiPoint' \
               or geom_type == 'GeometryCollection'
