# Compile GeoSpark source code
Some GeoSpark hackers may want to change some source code to fit in their own scenarios. To compile GeoSpark source code, you first need to download GeoSpark source code:

* Download / Git clone GeoSpark source code from [GeoSpark Github repo](https://github.com/DataSystemsLab/GeoSpark).

GeoSpark is a a project with three modules, core, sql, and viz. Each module is a Scala/Java mixed project which is managed by Apache Maven 3. 

* Make sure your machine has Java 1.8 and Apache Maven 3.

To compile all modules, please make sure you are in the root folder of three modules. Then enter the following command in the terminal:

```
mvn clean install -DskipTests
```
This command will first delete the old binary files and compile all three modules. This compilation will skip the unit tests of GeoSpark.

To compile a module of GeoSpark, please make sure you are in the folder of that module. Then enter the same command.

To run unit tests, just simply remove `-DskipTests` option. The command is like this:
```
mvn clean install
```

Note, the unit tests of all three modules may take up to 20 minutes. 