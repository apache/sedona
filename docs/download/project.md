# Self-contained Spark projects

A self-contained project allows you to create multiple Scala / Java files and write complex logics in one place. To use Sedona in your self-contained Spark project, you just need to add Sedona as a dependency in your POM.xml or build.sbt.

## Quick start

1. To add Sedona as dependencies, please read [Sedona Maven Central coordinates](maven-coordinates.md)
2. Use Sedona Template project to start: [Sedona Template Project](/tutorial/demo/)
3. Compile your project using SBT. Make sure you obtain the fat jar which packages all dependencies.
4. Submit your compiled fat jar to Spark cluster. Make sure you are in the root folder of Spark distribution. Then run the following command:
```
./bin/spark-submit --master spark://YOUR-IP:7077 /Path/To/YourJar.jar
```

!!!note
	The detailed explanation of spark-submit is available on [Spark website](https://spark.apache.org/docs/latest/submitting-applications.html).