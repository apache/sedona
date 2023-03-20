# Sedona-Analysis

This is a template project that shows how to use Sedona in Spatial Data Mining

## Example

### Spatial Co-Location Pattern Mining

#### What is spatial co-location pattern mining
Spatial co-location is defined as two or more species are often located in a neighborhood relationship. Ripley's K function is often used in judging co-location. It usually executes multiple times and form a 2-dimension curve for observation.

***In Africa, lions co-locate with zebras.***

<img src="https://github.com/jiayuasu/GeoSparkTemplateProject/raw/master/geospark-analysis/src/test/resources/colocation.png" width="300">

#### Ripley's K function
We use Ripley's K function to calculate **Multivariate Spatial Patterns**

Here are some materials regarding how to use Ripley's K function and its transformation L function.

Single type K function:

* [ArcGIS documents](http://pro.arcgis.com/en/pro-app/tool-reference/spatial-statistics/h-how-multi-distance-spatial-cluster-analysis-ripl.htm)
* Ripley's paper: Ripley, B.D. (1976). The second-order analysis of stationary point processes, Journal of Applied Probability
13, 255–266.

Multivariate K function and L function:

* [Philip M. Dixon, Encyclopedia of Environmetrics](https://www3.nd.edu/~mhaenggi/ee87021/Dixon-K-Function.pdf)
* [A blog about the co-location of Pokémon and Pokéstops in PokémonGo](http://blog.jlevente.com/understanding-the-cross-k-function/)

#### New York City Taxi Trip and Area Landmarks

The data scientist in NYC Taxi Company has a guess that the taxi pickup points are co-located with these area landmarks such as airports, museums, hospitals, colleges and so on. In other words, many taxi trips start from area landmarks. He wants to use a quantitative metric to measure the degree of co-location pattern.

**Dataset**

* NYC Taxi trip pickup points (CSV): http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
* NYC Area landmarks (Shapefile): https://geo.nyu.edu/catalog/nyu_2451_34514

**Code**

Run the code in "ScalaExample". You will obtain a visualized co-location map and the result of 10 iterations Ripley's L function.

You can download more NYC taxi trip data to obtain a detailed analyze result.

**Result**

Visualized co-location map, use the subset in the template project. The output image is in the root folder:

<img src="https://github.com/jiayuasu/GeoSparkTemplateProject/raw/master/geospark-analysis/src/test/resources/colocationMap.png" width="300">

Visualized co-location map, use all 1.3 billion taxi trip pickup points:

<img src="https://github.com/jiayuasu/GeoSparkTemplateProject/raw/master/geospark-analysis/src/test/resources/nyccolocation.png" width="300">

Ripley's L function result:

<img src="https://github.com/jiayuasu/GeoSparkTemplateProject/raw/master/geospark-analysis/src/test/resources/colocationResult.png" width="500">

Conclusion:
New York City taxi trip pickup points co-locate with New York City area landmarks