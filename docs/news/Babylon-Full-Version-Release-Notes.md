
##  Version information


|      Version     	| Summary                                                                                                                                                                                                               	|
|:----------------:	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|0.2.2| **Add the support of new output storage**: Now the user is able to output gigapixel or megapixel resolution images (image tiles or stitched single image) to HDFS and Amazon S3. Please use the new ImageGenerator not the BabylonImageGenerator class.|
|0.2.1| **Performance enhancement**: significantly accelerate single image generation pipeline. **Bug fix**:fix a bug in scatter plot parallel rendering.|
|0.2.0| **API updates for [Issue #80](https://github.com/DataSystemsLab/GeoSpark/issues/80):** 1. Babylon now has two different OverlayOperators for raster image and vector image: RasterOverlayOperator and VectorOverlayOperator; 2. Babylon merged old SparkImageGenerator and NativeJavaGenerator into a new BabylonImageGenerator which has neat APIs; **New feature:** Babylon can use Scatter Plot to visualize NASA Petabytes NetCDF/HDF format Earth Data. ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/babylon/src/main/scala/org/datasyslab/geospark/showcase),[Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/babylon/src/main/java/org/datasyslab/babylon/showcase))
|0.1.1| **Major updates:** Babylon supports vector image and outputs SVG image format|
|0.1.0| **Major updates:** Babylon initial version supports raster images|				|
|   Master branch  	| Contain latest code                                                                                                                                                                                                   	 	|
| Spark 1.X branch 	| Contain latest code but only supports Apache Spark 1.X        																																								|
