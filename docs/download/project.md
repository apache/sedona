# Self-contained Spark projects
A self-contained project allows you to create multiple Scala / Java files and write complex logics in one place. To use GeoSpark in your self-contained Spark project, you just need to add GeoSpark as a dependency in your POM.xml or build.sbt.

1. To add GeoSpark as dependencies, please read [GeoSpark Maven Central coordinates](../news/GeoSpark-All-Modules-Maven-Central-Coordinates.md)
2. Use GeoSpark Template project to start: [GeoSpark Template Project](https://github.com/jiayuasu/GeoSparkTemplateProject)
3. Compile your project using SBT or Maven. Make sure you obtain the fat jar which packages all dependencies.
4. Submit your compiled fat jar to Spark cluster. Make sure you are in the root folder of Spark distribution. Then run the following command:
```
./bin/spark-submit --master spark://YOUR-IP:7077 /Path/To/YourJar.jar
```

The detailed explanation of spark-submit is available on [Spark website](https://spark.apache.org/docs/latest/submitting-applications.html).