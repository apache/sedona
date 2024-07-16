SedonaPyDeck offers a number of APIs which aid in quick and interactive visualization of a geospatial data in a Jupyter notebook/lab environment.

Inorder to start using SedonaPyDeck, simply import Sedona using:

```python
from sedona.spark import *
```

Alternatively it can also be imported using:

```python
from sedona.maps.SedonaPyDeck import SedonaPyDeck
```

!!!Note
    For more information on the optional parameters please visit [PyDeck docs](https://deckgl.readthedocs.io/en/latest/deck.html).

    SedonaPyDeck assumes the map provider to be Mapbox when user selects 'salellite' option for `map_style`.

Following are details on all the APIs exposed via SedonaPyDeck:

### **Geometry Map**

```python
def create_geometry_map(df, fill_color="[85, 183, 177, 255]", line_color="[85, 183, 177, 255]",
                   elevation_col=0, initial_view_state=None,
                   map_style=None, map_provider=None, api_keys=None, stroked=True):
```

The parameter `fill_color` can be given a list of RGB/RGBA values, or a string that contains RGB/RGBA values based on a column, and is used to color polygons or point geometries in the map

The parameter `line_color` can be given a list of RGB/RGBA values, or a string that contains RGB/RGBA values based on a column, and is used to color the line geometries in the map.

The parameter `elevation_col` can be given a static elevation or elevation based on column values like `fill_color`, this only works for the polygon geometries in the map.

The parameter `stroked` determines whether to draw an outline around polygons and points, accepts a boolean value. For more information, please refer to this [documentation of deck.gl](https://deck.gl/docs/api-reference/layers/geojson-layer#:~:text=%27circle%27.-,stroked,-(boolean%2C%20optional)).

Optionally, parameters `initial_view_state`, `map_style`, `map_provider`, `api_keys` can be passed to configure the map as per user's liking.
More details on the parameters and their default values can be found on the PyDeck website as well by deck.gl [here](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/layers/geojson-layer.md)

### **Choropleth Map**

```python
def create_choropleth_map(df, fill_color=None, plot_col=None, initial_view_state=None, map_style=None,
						  map_provider=None, api_keys=None, elevation_col=0, stroked=True)
```

The parameter `fill_color` can be given a list of RGB/RGBA values, or a string that contains RGB/RGBA values based on a column.

The parameter `stroked` determines whether to draw an outline around polygons and points, accepts a boolean value. For more information please refer to this [documentation of deck.gl](https://deck.gl/docs/api-reference/layers/geojson-layer#:~:text=%27circle%27.-,stroked,-(boolean%2C%20optional)).

For example, all these are valid values of fill_color:

```python
fill_color=[255, 12, 250]
fill_color=[0, 12, 250, 255]
fill_color='[0, 12, 240, AirportCount * 10]' ## AirportCount is a column in the passed df
```

Instead of giving a `fill_color` parameter, a 'plot_col' can be passed which specifies the column to decide the choropleth.
SedonaPyDeck then creates a default color scheme based on the values of the column passed.

The parameter `elevation_col` can be given a numeric or a string value (containing the column with/without operations on it) to set a 3D elevation to the plotted polygons if any.

Optionally, parameters `initial_view_state`, `map_style`, `map_provider`, `api_keys` can be passed to configure the map as per user's liking.
More details on the parameters and their default values can be found on the PyDeck website.

### **Scatterplot**

```python
def create_scatterplot_map(df, fill_color="[255, 140, 0]", radius_col=1, radius_min_pixels = 1, radius_max_pixels = 10, radius_scale=1, initial_view_state=None,
                           map_style=None, map_provider=None, api_keys=None)
```

The parameter `fill_color` can be given a list of RGB/RGBA values, or a string that contains RGB/RGBA values based on a column.

The parameter `radius_col` can be given a numeric value or a string value consisting of any operations on the column, in order to specify the radius of the plotted point.

The parameter `radius_min_pixels` can be given a numeric value that would set the minimum radius in pixels. This can be used to prevent the plotted circle from getting too small when zoomed out.

The parameter `radius_max_pixels` can be given a numeric value that would set the maximum radius in pixels. This can be used to prevent the circle from getting too big when zoomed in.

The parameter `radius_scale` can be given a numeric value that sets a global radius multiplier for all points.

Optionally, parameters `initial_view_state`, `map_style`, `map_provider`, `api_keys` can be passed to configure the map as per user's liking.
More details on the parameters and their default values can be found on the PyDeck website as well by deck.gl [here](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/layers/scatterplot-layer.md)

### **Heatmap**

```python
def create_heatmap(df, color_range=None, weight=1, aggregation="SUM", initial_view_state=None, map_style=None,
                map_provider=None, api_keys=None)
```

The parameter `color_range` can be optionally given a list of RGB values, SedonaPyDeck by default uses `6-class YlOrRd` as color_range.
More examples can be found on [colorbrewer](https://colorbrewer2.org/#type=sequential&scheme=YlOrRd&n=6)

The parameter `weight` can be given a numeric value or a string with column and operations on it to determine weight of each point while plotting a heatmap.
By default, SedonaPyDeck assigns a weight of 1 to each point

The parameter `aggregation` can be used to define aggregation strategy to use when aggregating heatmap to a lower resolution (zooming out).
One of "MEAN" or "SUM" can be provided. By default, SedonaPyDeck uses "MEAN" as the aggregation strategy.

Optionally, parameters `initial_view_state`, `map_style`, `map_provider`, `api_keys` can be passed to configure the map as per user's liking.
More details on the parameters and their default values can be found on the PyDeck website as well by deck.gl [here](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/aggregation-layers/heatmap-layer.md)
