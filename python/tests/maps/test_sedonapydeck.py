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

from sedona.maps.SedonaPyDeck import SedonaPyDeck
from tests.test_base import TestBase
from tests import google_buildings_input_location
from tests import chicago_crimes_input_location
import pydeck as pdk
import geopandas as gpd
import json


class TestVisualization(TestBase):

    def testChoroplethMap(self):
        buildings_csv_df = self.spark.read.format("csv"). \
            option("delimiter", ","). \
            option("header", "true"). \
            option("inferSchema", "true"). \
            load(google_buildings_input_location)
        buildings_csv_df.createOrReplaceTempView("buildings_table")
        buildings_df = self.spark.sql(
            "SELECT confidence, latitude, longitude, ST_GeomFromWKT(geometry) as geometry from buildings_table"
        )
        buildings_gdf = gpd.GeoDataFrame(data=buildings_df.toPandas(), geometry="geometry")
        fill_color = SedonaPyDeck._create_default_fill_color_(gdf=buildings_gdf, plot_col='confidence')
        choropleth_layer = pdk.Layer(
            'GeoJsonLayer',  # `type` positional argument is here
            data=buildings_gdf,
            auto_highlight=True,
            get_fill_color=fill_color,
            opacity=1.0,
            stroked=True,
            extruded=True,
            wireframe=True,
            get_elevation=0,
            pickable=True
        )
        p_map = pdk.Deck(layers=[choropleth_layer], map_style='satellite', map_provider='mapbox')
        sedona_pydeck_map = SedonaPyDeck.create_choropleth_map(df=buildings_df, plot_col='confidence', map_style='satellite')
        res = self.isMapEqual(sedona_map=sedona_pydeck_map, pydeck_map=p_map)
        assert res is True

    def testPolygonMap(self):
        buildings_csv_df = self.spark.read.format("csv"). \
            option("delimiter", ","). \
            option("header", "true"). \
            load(google_buildings_input_location)
        buildings_csv_df.createOrReplaceTempView("buildings_table")
        buildings_df = self.spark.sql(
            "SELECT confidence, latitude, longitude, ST_GeomFromWKT(geometry) as geometry from buildings_table"
        )
        buildings_gdf = gpd.GeoDataFrame(data=buildings_df.toPandas(), geometry="geometry")
        polygon_layer = pdk.Layer(
            'GeoJsonLayer',  # `type` positional argument is here
            data=buildings_gdf,
            auto_highlight=True,
            get_fill_color="[85, 183, 177, 255]",
            opacity=0.4,
            stroked=True,
            extruded=True,
            get_elevation='confidence * 10',
            pickable=True,
            get_line_color="[85, 183, 177, 255]",
            get_line_width=3
        )
        p_map = pdk.Deck(layers=[polygon_layer])
        sedona_pydeck_map = SedonaPyDeck.create_geometry_map(df=buildings_df, elevation_col='confidence * 10')
        res = self.isMapEqual(sedona_map=sedona_pydeck_map, pydeck_map=p_map)
        assert res is True

    def testScatterplotMap(self):
        chicago_crimes_csv_df = self.spark.read.format("csv"). \
            option("delimiter", ","). \
            option("header", "true"). \
            load(chicago_crimes_input_location)
        chicago_crimes_csv_df.createOrReplaceTempView("crimes_table")
        chicago_crimes_df = self.spark.sql(
            "SELECT ST_POINT(CAST(x as DECIMAL(24, 20)), CAST (y as DECIMAL(24, 20))) as geometry, Description, "
            "Year from crimes_table")
        chicago_crimes_gdf = gpd.GeoDataFrame(data=chicago_crimes_df.toPandas(), geometry='geometry')
        SedonaPyDeck._create_coord_column_(chicago_crimes_gdf, geometry_col='geometry')
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=chicago_crimes_gdf,
            pickable=True,
            opacity=0.8,
            filled=True,
            get_position='coordinate_array_sedona',
            get_fill_color="[255, 140, 0]",
            get_radius=1,
            radius_min_pixels=1,
            radius_max_pixels=10,
            radius_scale=1
        )

        p_map = pdk.Deck(layers=[layer], map_style='satellite', map_provider='google_maps')
        sedona_pydeck_map = SedonaPyDeck.create_scatterplot_map(df=chicago_crimes_df, map_style='satellite', map_provider='google_maps')
        assert self.isMapEqual(sedona_map=sedona_pydeck_map, pydeck_map=p_map)

    def testHeatmap(self):
        chicago_crimes_csv_df = self.spark.read.format("csv"). \
            option("delimiter", ","). \
            option("header", "true"). \
            load(chicago_crimes_input_location)
        chicago_crimes_csv_df.createOrReplaceTempView("crimes_table")
        chicago_crimes_df = self.spark.sql("SELECT ST_POINT(CAST(x as DECIMAL(24, 20)), CAST (y as DECIMAL(24, "
                                           "20))) as geometry, Description, Year from crimes_table")
        chicago_crimes_gdf = gpd.GeoDataFrame(data=chicago_crimes_df.toPandas(), geometry='geometry')
        SedonaPyDeck._create_coord_column_(chicago_crimes_gdf, geometry_col='geometry')
        color_range = [
            [255, 255, 178],
            [254, 217, 118],
            [254, 178, 76],
            [253, 141, 60],
            [240, 59, 32],
            [240, 59, 32]
        ]
        aggregation = pdk.types.String("SUM")
        layer = pdk.Layer(
            "HeatmapLayer",
            chicago_crimes_gdf,
            pickable=True,
            opacity=0.8,
            filled=True,
            get_position='coordinate_array_sedona',
            aggregation=aggregation,
            color_range=color_range,
            get_weight=1
        )

        p_map = pdk.Deck(layers=[layer], map_style='satellite', map_provider='mapbox')
        sedona_pydeck_map = SedonaPyDeck.create_heatmap(df=chicago_crimes_df, map_style='satellite')
        assert self.isMapEqual(sedona_map=sedona_pydeck_map, pydeck_map=p_map)

    def isMapEqual(self, pydeck_map, sedona_map):
        sedona_dict = json.loads(sedona_map.to_json())
        pydeck_dict = json.loads(pydeck_map.to_json())
        if len(sedona_dict.keys()) != len(pydeck_dict.keys()):
            return False
        res = True
        for key in sedona_dict:
            try:
                if key == 'initialViewState':
                    continue
                if key == 'layers':
                    res &= self.isLayerEqual(pydeck_layer=pydeck_dict[key], sedona_layer=sedona_dict[key])
                    if res is False:
                        return False
                    continue
                res &= (str(pydeck_dict[key]) == str(sedona_dict[key]))
                if res is False:
                    return False
            except KeyError:
                return False

        return res

    def isLayerEqual(self, pydeck_layer, sedona_layer):
        if len(pydeck_layer) != len(sedona_layer):
            return False
        res = True
        for i in range(len(pydeck_layer)):
            if len(pydeck_layer[i].keys()) != len(sedona_layer[i].keys()):
                return False
            try:
                for key in pydeck_layer[i]:
                    if key == 'data' or key == 'id':
                        continue
                    res &= (str(pydeck_layer[i][key]) == str(sedona_layer[i][key]))
                    if res is False:
                        return False
            except KeyError:
                return False

        return res
