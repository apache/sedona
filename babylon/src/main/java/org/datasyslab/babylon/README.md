# BABYLON
**Babylon is a large-scale in-memory geospatial visualization system**

**Babylon** provides native support for general cartographic design  by extending **GeoSpark** to process large-scale spatial data. 
## Babylon Gallery
### Scatter Plot: USA mainland rail network
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/usrail.png" width="500">
### Heat Map: New York City Taxi Trips (with a given map background)
<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/nycheatmap.png" width="500">
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
* PNG
* JPG
* GIF
* More!

You also can support your desired image type by easily extending image generator! (JPG format is temporarily unavailable due to the lack of OpenJDK support)



### Current Visualization effect

* Scatter Plot
* Heat Map
* Choropleth Map
* More!

You also can build your new self-designed effects by easily extending the visualization operator!

### Example
Here is [a runnable single machine exmaple code](https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/babylon/showcase/Example.java). You can clone this repository and directly run it on you local machine!

### Scala and Java API
Please refer to [Babylon Scala and Java API](http://www.public.asu.edu/~jiayu2/geospark/javadoc/latest/).
### Supported Spatial Objects and Input format

All spatial obects and input formats supported by GeoSpark

##Contributor
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (jiayu2@asu.edu)
* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (msarwat@asu.edu)