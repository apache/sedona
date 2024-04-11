Before starting the Sedona journey, you need to make sure your Apache Flink cluster is ready.

Then you can create a self-contained Scala / Java project. A self-contained project allows you to create multiple Scala / Java files and write complex logics in one place.

To use Sedona in your self-contained Flink project, you just need to add Sedona as a dependency in your pom.xml or build.sbt.

1. To add Sedona as dependencies, please read [Sedona Maven Central coordinates](../maven-coordinates.md)
2. Read [Sedona Flink guide](../../tutorial/flink/sql.md) and use Sedona Template project to start: [Sedona Template Project](../../tutorial/demo.md)
3. Compile your project using Maven. Make sure you obtain the fat jar which packages all dependencies.
4. Submit your compiled fat jar to Flink cluster. Make sure you are in the root folder of Flink distribution. Then run the following command:

```
./bin/flink run /Path/To/YourJar.jar
```
