# Maven Coordinates

Sedona has four modules: `sedona-core, sedona-sql, sedona-viz, sedona-python-adapter`. They have different packing policies. You will need to use `sedona-python-adapter` for Scala, Java and Python API.  ==You may also need geotools-wrapper (see below)==. If you want to use SedonaViz, you will include one more jar: `sedona-viz`.

## Use Sedona fat jars

=== "Spark 3.0 + Scala 2.12"

	```xml
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-python-adapter-3.0_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-viz-3.0_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	```

=== "Spark 2.4 + Scala 2.11"

	```xml
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-python-adapter-2.4_2.11</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-viz-2.4_2.11</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	```
	
=== "Spark 2.4 + Scala 2.12"

	```xml
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-python-adapter-2.4_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-viz-2.4_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	```

### GeoTools 24.0+

GeoTools library is required only if you want to use CRS transformation and ShapefileReader. This wrapper library is a re-distriution of GeoTools official jars. The only purpose of this library is to bring GeoTools jars from OSGEO repository to Maven Central. This libary is under GNU Lesser General Public License (LGPL) license so we cannot package it in Sedona official release.

```xml
<!-- https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
<dependency>
    <groupId>org.datasyslab</groupId>
    <artifactId>geotools-wrapper</artifactId>
    <version>{{ sedona.current_geotools }}</version>
</dependency>
```

### SernetCDF 0.1.0

For Scala / Java API, it is required only if you want to read HDF/NetCDF files.

HDF/NetCDF function is currently not supported in Sedona Python.

Under Apache License 2.0.

```xml
<!-- https://mvnrepository.com/artifact/org.datasyslab/sernetcdf -->
<dependency>
    <groupId>org.datasyslab</groupId>
    <artifactId>sernetcdf</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Use Sedona and third-party jars separately

==For Scala and Java users==, if by any chance you don't want to use an uber jar that includes every dependency, you can use the following jars instead. ==Otherwise, please do not continue reading this section.==

=== "Spark 3.0 + Scala 2.12"

	```xml
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-core-3.0_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-sql-3.0_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-viz-3.0_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	```

=== "Spark 2.4 + Scala 2.11"

	```xml
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-core-2.4_2.11</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-sql-2.4_2.11</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-viz-2.4_2.11</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	```

=== "Spark 2.4 + Scala 2.12"

	```xml
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-core-2.4_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-sql-2.4_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	<dependency>
	  <groupId>org.apache.sedona</groupId>
	  <artifactId>sedona-viz-2.4_2.12</artifactId>
	  <version>{{ sedona.current_version }}</version>
	</dependency>
	```

### LocationTech JTS-core 1.18.0+

Under Eclipse Public License 2.0 ("EPL") or the Eclipse Distribution License 1.0 (a BSD Style License)

```xml
<!-- https://mvnrepository.com/artifact/org.locationtech.jts/jts-core -->
<dependency>
    <groupId>org.locationtech.jts</groupId>
    <artifactId>jts-core</artifactId>
    <version>1.18.0</version>
</dependency>
```

### jts2geojson 0.16.1+

Under MIT License. Please make sure you exclude jts and jackson from this library.

```xml
<!-- https://mvnrepository.com/artifact/org.wololo/jts2geojson -->
<dependency>
    <groupId>org.wololo</groupId>
    <artifactId>jts2geojson</artifactId>
    <version>0.16.1</version>
    <exclusions>
        <exclusion>
            <groupId>org.locationtech.jts</groupId>
            <artifactId>jts-core</artifactId>
        </exclusion>
        <exclusion>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

### GeoTools 24.0+

GeoTools library is required only if you want to use CRS transformation and ShapefileReader. This wrapper library is a re-distriution of GeoTools official jars. The only purpose of this library is to bring GeoTools jars from OSGEO repository to Maven Central. This libary is under GNU Lesser General Public License (LGPL) license so we cannot package it in Sedona official release.

```xml
<!-- https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
<dependency>
    <groupId>org.datasyslab</groupId>
    <artifactId>geotools-wrapper</artifactId>
    <version>{{ sedona.current_geotools }}</version>
</dependency>
```

### SernetCDF 0.1.0

For Scala / Java API: `required` if you want to read HDF files.

Under Apache License 2.0.

```xml
<!-- https://mvnrepository.com/artifact/org.datasyslab/sernetcdf -->
<dependency>
    <groupId>org.datasyslab</groupId>
    <artifactId>sernetcdf</artifactId>
    <version>0.1.0</version>
</dependency>
```

## SNAPSHOT versions
Sometimes Sedona has a SNAPSHOT version for the upcoming release. It follows the same naming conversion but has "SNAPSHOT" as suffix in the version. For example, `{{ sedona.current_snapshot }}`

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
