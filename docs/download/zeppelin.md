# Install GeoSpark-Zeppelin

!!!warning
	**Known issue**: due to an issue in Leaflet JS, GeoSpark-core can only plot each geometry (point, line string and polygon) as a point on Zeppelin map. To enjoy the scalable and full-fleged visualization, please use GeoSparkViz to plot scatter plots and heat maps on Zeppelin map.

## Compatibility

Apache Spark 2.1+

Apache Zeppelin 0.8.1+

GeoSpark 1.2.0+: GeoSpark-core, GeoSpark-SQL, GeoSpark-Viz

## Installation

!!!note
	You only need to do Step 1 and 2 only if you cannot see GeoSpark-Zeppelin in Zeppelin Helium package list.

### Create Helium folder (optional)
Create a folder called **helium** in Zeppelin root folder.

### Add GeoSpark-Zeppelin description (optional)

Create a file called **geospark-zeppelin.json** in this folder and put the following content in this file. You need to change the artifact path!

```
{
  "type": "VISUALIZATION",
  "name": "geospark-zeppelin",
  "description": "Zeppelin visualization support for GeoSpark",
  "artifact": "/Absolute/Path/GeoSpark/geospark-zeppelin",
  "license": "BSD-2-Clause",
  "icon": "<i class='fa fa-globe'></i>"
}
```
	
### Enable GeoSpark-Zeppelin

Restart Zeppelin then open Zeppelin Helium interface and enable GeoSpark-Zeppelin.

![Enable Package](../image/enable-helium.gif)

### Add GeoSpark dependencies in Zeppelin Spark Interpreter
![add-geospark](../image/add-geospark-interpreter.gif)


### Visualize GeoSparkSQL results

![sql-zeppelin](../image/sql-zeppelin.gif)

### Display GeoSparkViz results
![viz-zeppelin](../image/viz-zeppelin.gif)

Now, you are good to go! Please read [GeoSpark-Zeppelin tutorial](../tutorial/zeppelin.md) for a hands-on tutorial.
