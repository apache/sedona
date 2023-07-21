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


class TestVisualization(TestBase):
    def testPrepareDf(self):
        buildings_csv_df = self.spark.read.format("csv"). \
            option("delimiter", ","). \
            option("header", "true"). \
            load(google_buildings_input_location)
        buildings_csv_df.createOrReplaceTempView("buildings_table")
        buildings_df = self.spark.sql(
            "SELECT confidence, latitude, longitude, ST_GeomFromWKT(geometry) as geometry from buildings_table"
        )
        buildings_gdf = gpd.GeoDataFrame(data=buildings_df.toPandas(), geometry="geometry")
        sedona_pydeck_gdf = SedonaPyDeck.prepare_df(df=buildings_df)
        assert buildings_gdf.to_json() == sedona_pydeck_gdf.to_json()



    def testPrepareDfAddCoord(self):
        chicago_crimes_csv_df = self.spark.read.format("csv"). \
             option("delimiter", ","). \
             option("header", "true"). \
             load(chicago_crimes_input_location)
        chicago_crimes_csv_df.createOrReplaceTempView("crimes_table")
        chicago_crimes_df = self.spark.sql("SELECT ST_POINT(CAST(x as DECIMAL(24, 20)), CAST (y as DECIMAL(24, 20))) as geometry, Description, Year from crimes_table")
        chicago_crimes_gdf = gpd.GeoDataFrame(data=chicago_crimes_df.toPandas(), geometry='geometry')
        SedonaPyDeck._create_coord_column(chicago_crimes_gdf, geometry_col='geometry')
        sedona_pydeck_gdf = SedonaPyDeck.prepare_df(df=chicago_crimes_df, add_coords=True)
        assert chicago_crimes_gdf.to_json() == sedona_pydeck_gdf.to_json()


    def testChoroplethMap(self):
        buildings_csv_df = self.spark.read.format("csv"). \
            option("delimiter", ","). \
            option("header", "true"). \
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
            stroked=False,
            extruded=False,
            wireframe=True,
            pickable=True
        )

        p_map = pdk.Deck(layers=[choropleth_layer], map_style='light')
        sedona_pydeck_map = SedonaPyDeck.create_choropleth_map(df=buildings_df, plot_col='confidence')
        assert p_map.to_html() == sedona_pydeck_map.to_html()


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
            get_fill_color="[135, 206, 250, 255]",
            opacity=1.0,
            stroked=False,
            extruded=True,
            get_elevation='confidence * 10',
            wireframe=True,
            pickable=True
        )

        p_map = pdk.Deck(layers=[polygon_layer], map_style='light')
        sedona_pydeck_map = SedonaPyDeck.create_polygon_map(df=buildings_df, elevation_col='confidence * 10')
        assert p_map.to_html() == sedona_pydeck_map.to_html()


    def testScatterplotMap(self):
        chicago_crimes_csv_df = self.spark.read.format("csv"). \
            option("delimiter", ","). \
            option("header", "true"). \
            load(chicago_crimes_input_location).sample(frac=0.5)
        chicago_crimes_csv_df.createOrReplaceTempView("crimes_table")
        chicago_crimes_df = self.spark.sql("SELECT ST_POINT(CAST(x as DECIMAL(24, 20)), CAST (y as DECIMAL(24, 20))) as geometry, Description, Year from crimes_table")
        chicago_crimes_gdf = gpd.GeoDataFrame(data=chicago_crimes_df.toPandas(), geometry='geometry')
        SedonaPyDeck._create_coord_column(chicago_crimes_gdf, geometry_col='geometry')
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=chicago_crimes_gdf,
            pickable=True,
            opacity=0.8,
            filled=True,
            get_position='coordinate_array_sedona',
            get_fill_color=[255, 140, 0],
        )

        p_map = pdk.Deck(layers=[layer], map_style='dark')
        sedona_pydeck_map = SedonaPyDeck.create_scatterplot_map(df=chicago_crimes_df)
        assert p_map.to_html() == sedona_pydeck_map.to_html()


    def testHeatmap(self):
        chicago_crimes_csv_df = self.spark.read.format("csv"). \
            option("delimiter", ","). \
            option("header", "true"). \
            load(chicago_crimes_input_location).sample(frac=0.5)
        chicago_crimes_csv_df.createOrReplaceTempView("crimes_table")
        chicago_crimes_df = self.spark.sql("SELECT ST_POINT(CAST(x as DECIMAL(24, 20)), CAST (y as DECIMAL(24, 20))) as geometry, Description, Year from crimes_table")
        chicago_crimes_gdf = gpd.GeoDataFrame(data=chicago_crimes_df.toPandas(), geometry='geometry')
        SedonaPyDeck._create_coord_column(chicago_crimes_gdf, geometry_col='geometry')
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
            aggregation = aggregation,
            color_range=color_range,
            get_weight=1
        )


        p_map = pdk.Deck(layers=[layer])
        sedona_pydeck_map = SedonaPyDeck.create_heatmap(df=chicago_crimes_df)
        assert p_map.to_html() == sedona_pydeck_map.to_html()


