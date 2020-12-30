# Maven Coordinates

Sedona has four modules: `sedona-core, sedona-sql, sedona-viz, sedona-python-adapter`. If you use Scala and Java API, you only need to use `sedona-core, sedona-sql, sedona-viz`. If you use Python API, you only need to use `sedona-python-adapter`

!!!note
	Sedona Scala and Java API also requires additional dependencies to work (see below). Python API does not need them.

## Spark 3.0 + Scala 2.12

Scala and Java API only
```xml
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-core_2.12</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-sql-3.0_2.12</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-viz-3.0_2.12</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
```

Python API only
```xml
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-python-adapter-3.0_2.12</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
```

## Spark 2.4 + Scala 2.11

Scala and Java API only
```xml
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-core_2.11</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-sql-2.4_2.11</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-viz-2.4_2.11</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
```

Python API only
```xml
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-python-adapter-2.4_2.11</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
```

## Spark 2.4 + Scala 2.12

Scala and Java API only
```xml
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-core_2.12</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-sql-2.4_2.12</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-viz-2.4_2.12</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
```

Python API only
```xml
<dependency>
  <groupId>org.apache.sedona</groupId>
  <artifactId>sedona-python-adapter-2.4_2.12</artifactId>
  <version>1.0.0-incubator</version>
</dependency>
```

## Additional dependencies

To avoid conflicts in downstream projects and solve the copyright issue, Sedona almost does not package any dependencies in the release jars. Therefore, you need to add the following jars in your `build.sbt` or `pom.xml` if you use Sedona Scala and Java API.

### LocationTech JTS-core 1.18.0+

```xml
<!-- https://mvnrepository.com/artifact/org.locationtech.jts/jts-core -->
<dependency>
    <groupId>org.locationtech.jts</groupId>
    <artifactId>jts-core</artifactId>
    <version>1.18.0</version>
</dependency>
```

### jts2geojson 0.14.3+

This is only needed if you read GeoJSON files. Under MIT License

```xml
<!-- https://mvnrepository.com/artifact/org.wololo/jts2geojson -->
<dependency>
    <groupId>org.wololo</groupId>
    <artifactId>jts2geojson</artifactId>
    <version>0.14.3</version>
</dependency>
```

### GeoTools 24.0+

This is only needed if you want to do CRS transformation. Under GNU Lesser General Public License (LGPL) license.

```xml
<!-- https://mvnrepository.com/artifact/org.geotools/gt-main -->
<dependency>
    <groupId>org.geotools</groupId>
    <artifactId>gt-main</artifactId>
    <version>24.0</version>
</dependency>
<!-- https://mvnrepository.com/artifact/org.geotools/gt-referencing -->
<dependency>
    <groupId>org.geotools</groupId>
    <artifactId>gt-referencing</artifactId>
    <version>24.0</version>
</dependency>
<!-- https://mvnrepository.com/artifact/org.geotools/gt-epsg-hsql -->
<dependency>
    <groupId>org.geotools</groupId>
    <artifactId>gt-epsg-hsql</artifactId>
    <version>24.0</version>
</dependency>
```

GeoTools libraries are in OSGeo repository instead of Maven Central. You need to add the repo in `build.sbt` or `pom.xml`

#### pom.xml
```xml
<repositories>
    <repository>
        <id>maven2-repository.dev.java.net</id>
        <name>Java.net repository</name>
        <url>https://download.java.net/maven/2</url>
    </repository>
    <repository>
        <id>osgeo</id>
        <name>OSGeo Release Repository</name>
        <url>https://repo.osgeo.org/repository/release/</url>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
        <releases>
            <enabled>true</enabled>
        </releases>
    </repository>
</repositories>
```

#### Build.sbt
```scala
resolvers +=
  "Open Source Geospatial Foundation Repository" at "https://repo.osgeo.org/repository/release/"

resolvers +=
  "Java.net repository" at "https://download.java.net/maven/2"
```

### SernetCDF 0.1.0

This is only needed if you want to read HDF files. Under Apache License 2.0.

```xml
<!-- https://mvnrepository.com/artifact/org.datasyslab/sernetcdf -->
<dependency>
    <groupId>org.datasyslab</groupId>
    <artifactId>sernetcdf</artifactId>
    <version>0.1.0</version>
</dependency>

```

## SNAPSHOT versions
Sometimes Sedona has a SNAPSHOT version for the upcoming release. It follows the same naming conversion but has "SNAPSHOT" as suffix in the version. For example, `1.0.0-incubator-SNAPSHOT`

In order to download SNAPSHOTs, you need to add the following repositories in your POM.XML or build.sbt
### build.sbt
resolvers +=
  "Apache Software Foundation Snapshots" at "https://repository.apache.org/content/groups/snapshots"
### POM.XML

```xml
<repositories>
    <repository>
        <id>snapshots-repo</id>
        <url>https://repository.apache.org/content/groups/snapshots</url>
        <releases><enabled>false</enabled></releases>
        <snapshots><enabled>true</enabled></snapshots>
    </repository>
</repositories>
```
