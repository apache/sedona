# GeoSparkViz: GeoSpark Visualization Extension


**Supported Apache Spark version:** `2.0+(Master branch)` `1.0+(1.X branch) `

**GeoSparkViz** in this repository **(v0.1.X-0.2.X)** provides native support for general cartographic design.

**GeoSparkViz** artifacts are hosted in Maven Central: [**Maven Central Coordinates**](https://github.com/DataSystemsLab/GeoSpark/wiki/Babylon-Maven-Central-Coordinates)

**GeoSparkViz-GeoSpark compatibility**: [**Reference Sheet**](https://github.com/DataSystemsLab/GeoSpark/wiki/Babylon-GeoSpark-compatibility)


##  Version release notes: [click here](https://github.com/DataSystemsLab/GeoSpark/wiki/Babylon-Full-Version-Release-notes)

## News!

This repository (formerly Babylon) has been renamed to GeoSparkViz (GeoSpark Visualization Extension). We apologize for any name inconsistency in documents or code.

## GeoSparkViz Gallery

### Scatter Plot: USA mainland rail network
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/usrail.png" width="500">

### Heat Map: New York City Taxi Trips (with a given map background)
[Watch High Resolution on a real map](http://www.public.asu.edu/~jiayu2/geospark/picture/overlay.html)

<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/heatmapnycsmall.png" width="500">
### Choropleth Map + Overlay Operator: USA mainland tweets per USA county (Spatial Join Query)
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/ustweet.png" width="500">

## Main Features

### Extensible Visualization operator (just like playing LEGO bricks)!

* Support super high resolution image generation: parallel map image rendering
* Visualize Spatial RDD and Spatial Queries (Spatial Range, Spatial K Nearest Neighbors, Spatial Join)
* Customizable: Can be customized to any user-supplied colors or coloring rule
* Extensible: Can be extended to any visualization effect

### Overlay Operator
Overlay one map layer with many other map layers!

### Various Image Filter
* Gaussian Blur
* Box Blur
* Embose
* Outline
* Sharpen
* More!

You also can buld your new image filter by easily extending the photo filter!

### Various Image Type
* Raster image: PNG
* Vector image: SVG (Only support Scatter plot and Choropleth Map)
* More!

You also can support your desired image type by easily extending image generator! (JPG format is temporarily unavailable due to the lack of OpenJDK support)



### Current Visualization effect

* Scatter Plot
* Heat Map
* Choropleth Map
* More!

### Current Output Storage

* Local disk
* Hadoop Distributed File System (HDFS)
* Amazon Simple Storage Service (Amazon S3) 

You also can build your new self-designed effects by easily extending the visualization operator!

# GeoSparkViz Tutorial ([more](https://github.com/DataSystemsLab/GeoSpark/wiki))
GeoSparkViz full tutorial is available at GeoSpark GitHub Wiki: [https://github.com/DataSystemsLab/GeoSpark/wiki](https://github.com/DataSystemsLab/GeoSpark/wiki)


### Supported Spatial Objects and Input format

All spatial obects and input formats supported by GeoSpark

# Acknowledgement

GeoSparkViz makes use of JFreeSVG plus (An extension of JFreeSVG 3.2) for some SVG image opertaions.

Please refer to [JFreeSVG website](http://www.jfree.org/jfreesvg) and [JFreeSVG plus](https://github.com/jiayuasu/jfreesvg) for more details.



# Contact

## Questions

* Please join [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

* Email us!

## Contact
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (Email: jiayu2@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

## Project website
Please visit [GeoSpark project wesbite](http://geospark.datasyslab.org) for latest news and releases.

## Data Systems Lab
GeoSparkViz is one of the projects under [Data Systems Lab](http://www.datasyslab.org/) at Arizona State University. The mission of Data Systems Lab is designing and developing experimental data management systems (e.g., database systems).
