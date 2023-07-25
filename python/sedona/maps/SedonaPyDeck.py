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

import pydeck as pdk
from sedona.maps.SedonaMapUtils import SedonaMapUtils


class SedonaPyDeck:

    # User Facing APIs
    @classmethod
    def create_choropleth_map(cls, df, fill_color=None, plot_col=None, initial_view_state=None, map_style=None,
                              map_provider=None, elevation_col=0):
        """
        Create a pydeck map with a choropleth layer added
        :param elevation_col: Optional elevation for the polygons
        :param df: SedonaDataFrame to plot on the choropleth map.
        :param fill_color: color scheme to fill the map with.
                If no color scheme is given, a default color scheme is created using the 'plot_col' column as the quantizing column
        :param plot_col: Column to be used to create a default color scheme. If fill_color is provided, this parameter is ignored.
        :param initial_view_state:
        :param map_style:
        :param map_provider:
        :return: A pydeck Map object with choropleth layer added:
        """

        if initial_view_state is None:
            gdf = SedonaPyDeck._prepare_df_(df, add_coords=True)
            initial_view_state = pdk.data_utils.compute_view(gdf['coordinate_array_sedona'])
        else:
            gdf = SedonaPyDeck._prepare_df_(df)

        if fill_color is None:
            fill_color = SedonaPyDeck._create_default_fill_color_(gdf, plot_col)

        choropleth_layer = pdk.Layer(
            'GeoJsonLayer',  # `type` positional argument is here
            data=gdf,
            auto_highlight=True,
            get_fill_color=fill_color,
            opacity=1.0,
            get_elevation=elevation_col,
            stroked=False,
            extruded=True,
            wireframe=True,
            pickable=True
        )

        map_ = pdk.Deck(layers=[choropleth_layer], initial_view_state=initial_view_state)
        SedonaPyDeck._set_optional_parameters_(p_map=map_, map_style=map_style, map_provider=map_provider)
        return map_

    @classmethod
    def create_geometry_map(cls, df, fill_color="[85, 183, 177, 255]", line_color="[85, 183, 177, 255]",
                            elevation_col=0, initial_view_state=None,
                            map_style=None, map_provider=None):
        """
        Create a pydeck map with a GeoJsonLayer added for plotting given geometries.
        :param line_color:
        :param df: SedonaDataFrame with polygons
        :param fill_color: Optional color for the plotted polygons
        :param elevation_col: Optional column/numeric value to determine elevation for plotted polygons
        :param initial_view_state: optional initial view state of the pydeck map
        :param map_style: optional map_style of the pydeck map
        :param map_provider: optional map_provider of the pydeck map
        :return: A pydeck map with a GeoJsonLayer map added
        """
        geometry_col = SedonaMapUtils.__get_geometry_col__(df)
        gdf = SedonaPyDeck._prepare_df_(df, geometry_col=geometry_col)
        geom_type = gdf[geometry_col][0].geom_type
        SedonaPyDeck._create_coord_column_(gdf, geometry_col=geometry_col)
        # if len(type_list) >= 2:  # change line colors if any to make it more visible.
        #     if line_color == "[85, 183, 177, 255]":
        #         line_color = "[237, 119, 79]"

        layer = SedonaPyDeck._create_fat_layer_(gdf, fill_color=fill_color, elevation_col=elevation_col,
                                                line_color=line_color)

        if initial_view_state is None:
            initial_view_state = pdk.data_utils.compute_view(gdf['coordinate_array_sedona'])

        if elevation_col != 0 and geom_type == "Polygon":
            initial_view_state.pitch = 45  # If polygons are elevated, change the pitch to visualize the elevation better
        map_ = pdk.Deck(layers=[layer], map_style='dark', initial_view_state=initial_view_state)
        SedonaPyDeck._set_optional_parameters_(p_map=map_, map_style=map_style, map_provider=map_provider)
        return map_

    @classmethod
    def create_scatterplot_map(cls, df, fill_color="[255, 140, 0]", radius_col=1, radius_min_pixels=1,
                               radius_max_pixels=10, radius_scale=1, initial_view_state=None, map_style=None,
                               map_provider=None):
        """
        Create a pydeck map with a scatterplot layer
        :param radius_scale:
        :param radius_max_pixels:
        :param radius_min_pixels:
        :param radius_col:
        :param df: SedonaDataFrame to plot
        :param fill_color: color of the points
        :param initial_view_state: optional initial view state of a pydeck map
        :param map_style: optional map_style to be added to the pydeck map
        :param map_provider: optional map_provider to be added to the pydeck map
        :return: A pydeck map object with a scatterplot layer added
        """
        gdf = SedonaPyDeck._prepare_df_(df, add_coords=True)
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=gdf,
            pickable=True,
            opacity=0.8,
            filled=True,
            get_radius=radius_col,
            radius_min_pixels=radius_min_pixels,
            radius_max_pixels=radius_max_pixels,
            radius_scale=radius_scale,
            get_position='coordinate_array_sedona',
            get_fill_color=fill_color,
        )

        if initial_view_state is None:
            initial_view_state = pdk.data_utils.compute_view(gdf['coordinate_array_sedona'])

        map_ = pdk.Deck(layers=[layer], initial_view_state=initial_view_state)
        SedonaPyDeck._set_optional_parameters_(p_map=map_, map_style=map_style, map_provider=map_provider)
        return map_

    @classmethod
    def create_heatmap(cls, df, color_range=None, weight=1, aggregation="SUM", initial_view_state=None, map_style=None,
                       map_provider=None):
        """
        Create a pydeck map with a heatmap layer added
        :param df: SedonaDataFrame to be used to plot the heatmap
        :param color_range: Optional color_range for the heatmap
        :param weight: Optional column to determine weight of each point while plotting the heatmap
        :param aggregation: Optional aggregation to use for the heatmap (used when rendering a zoomed out heatmap)
        :param initial_view_state: Optional initial view state of the pydeck map
        :param map_style: Optional map_style of the pydeck map
        :param map_provider: Optional map_provider for the pydeck map
        :return: A pydeck map with a heatmap layer added
        """

        gdf = SedonaPyDeck._prepare_df_(df, add_coords=True)

        if color_range is None:
            color_range = [
                [255, 255, 178],
                [254, 217, 118],
                [254, 178, 76],
                [253, 141, 60],
                [240, 59, 32],
                [240, 59, 32]
            ]

        layer = pdk.Layer(
            "HeatmapLayer",
            gdf,
            pickable=True,
            opacity=0.8,
            filled=True,
            get_position='coordinate_array_sedona',
            aggregation=pdk.types.String(aggregation),
            color_range=color_range,
            get_weight=weight
        )

        if initial_view_state is None:
            initial_view_state = pdk.data_utils.compute_view(gdf['coordinate_array_sedona'])

        map_ = pdk.Deck(layers=[layer], initial_view_state=initial_view_state)
        SedonaPyDeck._set_optional_parameters_(p_map=map_, map_style=map_style, map_provider=map_provider)

        return map_

    @classmethod
    def _prepare_df_(cls, df, add_coords=False, geometry_col=None):
        """
        Convert a SedonaDataFrame to a GeoPandas DataFrame without renaming the column (as it is not necessary while rendering a pydeck map)
        :param df: SedonaDataFrame
        :param add_coords: Used to decide if a coordinate column containing lists of point coordinates is to be added to the resultant gdf.
        :return: GeoPandas DataFrame
        """
        if geometry_col is None:
            geometry_col = SedonaMapUtils.__get_geometry_col__(df=df)
        gdf = SedonaMapUtils.__convert_to_gdf__(df, rename=False, geometry_col=geometry_col)
        if add_coords is True:
            SedonaPyDeck._create_coord_column_(gdf=gdf, geometry_col=geometry_col)
        return gdf

    # Utility APIs specific to SedonaPyDeck
    @classmethod
    def _set_optional_parameters_(cls, p_map, map_style, map_provider):
        """
        Set optional parameters to a given pydeck map
        (Parameters are self-explanatory for a pydeck map)
        :param p_map:
        :param map_style:
        :param map_provider:
        """
        if map_style is not None:
            p_map.map_style = map_style

        if map_provider is not None:
            p_map.map_provider = map_provider

    @classmethod
    def _create_default_fill_color_(cls, gdf, plot_col):
        """
        Create a default color for choropleth layer based on a given max value"
        :param plot_col:
        :return: fill_color string for pydeck map
        """
        plot_max = gdf[plot_col].max()
        return "[85, 183, 177, ({0} / {1}) * 255 + 15]".format(plot_col, plot_max)

    @classmethod
    def _create_coord_column_(cls, gdf, geometry_col, add_points=False):
        """
        Create a coordinate column in a given GeoPandas Dataframe, this coordinate column contains coordinates of a ST_Point in a list format of [longitude, latitude]
        :param gdf: GeoPandas Dataframe
        :param geometry_col: column with ST_Points
        """
        type_list = []
        gdf['coordinate_array_sedona'] = gdf.apply(
            lambda val: list(SedonaMapUtils.__extract_coordinate__(val[geometry_col], type_list)), axis=1)

    @classmethod
    def _create_fat_layer_(cls, gdf, fill_color, line_color, elevation_col):
        layer = pdk.Layer(
            'GeoJsonLayer',  # `type` positional argument is here
            data=gdf,
            auto_highlight=True,
            get_fill_color=fill_color,
            opacity=0.4,
            stroked=False,
            extruded=True,
            get_elevation=elevation_col,
            get_line_color=line_color,
            pickable=True,
            get_line_width=3
        )

        return layer
