# GeoSpark-Zeppelin #

GeoSpark-Zeppelin is a Helium visualization plugin for Apache Zeppelin, written in Node.js. After installing this plugin, you can directly connect GeoSparkSQL and GeoSparkViz to Apache Zeppelin.

GeoSpark-Zeppelin is a pluggable component of Zeppelin and requires no Zeppelin source modification.

## Compatibility ##

Apache Spark 2.1+

Apache Zeppelin 0.8.1

GeoSpark 1.2.0+: GeoSpark-core, GeoSpark-SQL, GeoSpark-Viz

# Installation
1. [Optional] Create a folder in Zeppelin installation root folder called **helium**
2. [Optional] Create a file called **geospark-zeppelin.json** in this folder and put the following content in this file. You need to change the artifact path!
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
3. Open Zeppelin Helium interface and enable GeoSpark-Zeppelin
![Enable Package](https://cloud.githubusercontent.com/assets/28304007/25633533/6c508d9e-2f45-11e7-99a7-505d94c382ba.gif)

4. Open a Zeppelin Spark notebook
![Settings](https://cloud.githubusercontent.com/assets/28304007/25633526/69752486-2f45-11e7-9963-358d5ff29165.gif)

5. Visualize GeoSparkSQL results
![sql-zeppelin](docs/api/image/sql-zeppelin.png)

6. Display GeoSparkViz results
![viz-zeppelin](docs/api/image/sql-zeppelin.png)

## Acknowledgement

This plugin uses [Leaflet](http://leafletjs.com/) and [JSTS](http://bjornharrtell.github.io/jsts/).
Some code is forked from [zeppelin-leaflet
](https://github.com/myuwono/zeppelin-leaflet).

## License ##

* geospark-zeppelin: BSD-2-Clause
* Leaflet: [License](https://github.com/Leaflet/Leaflet/blob/master/LICENSE) - BSD-2-Clause
