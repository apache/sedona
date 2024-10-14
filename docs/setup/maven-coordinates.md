# Maven Coordinates

## Use Sedona shaded (fat) jars

!!!warning
	For Scala/Java/Python users, this is the most common way to use Sedona in your environment. Do not use separate Sedona jars unless you are sure that you do not need shaded jars.

!!!warning
	For R users, this is the only way to use Sedona in your environment.

Apache Sedona provides different packages for each supported version of Spark.

* For Spark 3.0 to 3.3, the artifact to use should be `sedona-spark-shaded-3.0_2.12`.
* For Spark 3.4 or higher versions, please use the artifact with Spark major.minor version in the artifact name. For example, for Spark 3.4, the artifacts to use should be `sedona-spark-shaded-3.4_2.12`.

If you are using the Scala 2.13 builds of Spark, please use the corresponding packages for Scala 2.13, which are suffixed by `_2.13`.

The optional GeoTools library is required if you want to use CRS transformation, ShapefileReader or GeoTiff reader. This wrapper library is a re-distribution of GeoTools official jars. The only purpose of this library is to bring GeoTools jars from OSGEO repository to Maven Central. This library is under GNU Lesser General Public License (LGPL) license so we cannot package it in Sedona official release.

!!! abstract "Sedona with Apache Spark and Scala 2.12"

	=== "Spark 3.0 to 3.3 and Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.0_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- Optional: https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

	=== "Spark 3.4 and Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.4_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- Optional: https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.5 and Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.5_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- Optional: https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona with Apache Spark and Scala 2.13"

	=== "Spark 3.0 to 3.3 and Scala 2.13"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.0_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- Optional: https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

	=== "Spark 3.4 and Scala 2.13"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.4_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- Optional: https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.5 and Scala 2.13"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-shaded-3.5_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- Optional: https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona with Apache Flink"

	=== "Flink 1.12+ and Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-flink-shaded_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- Optional: https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona with Snowflake"

	=== "Snowflake 7.0+ (Year 2023 and later)"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-snowflake</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<!-- Optional: https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

### netCDF-Java 5.4.2

This is required only if you want to read HDF/NetCDF files using `RS_FromNetCDF`. Note that this JAR is not in Maven Central so you will need to add this repository to your pom.xml or build.sbt, or specify the URL in Spark Config `spark.jars.repositories` or spark-submit `--repositories` option.

!!!warning
	This jar was a required dependency due to a bug in Sedona 1.5.1. You will need to specify the URL of the repository in `spark.jars.repositories` if you use 1.5.1. This has been fixed in Sedona 1.5.2 and later.

Under BSD 3-clause (compatible with Apache 2.0 license)

!!! abstract "Add HDF/NetCDF dependency"

	=== "Sedona 1.3.1+"

		Add unidata repo to your pom.xml

		```
		<repositories>
		    <repository>
		        <id>unidata-all</id>
		        <name>Unidata All</name>
		        <url>https://artifacts.unidata.ucar.edu/repository/unidata-all/</url>
		    </repository>
		</repositories>
		```

		Then add cdm-core to your POM dependency.

		```xml
		<dependency>
		    <groupId>edu.ucar</groupId>
		    <artifactId>cdm-core</artifactId>
		    <version>5.4.2</version>
		</dependency>
		```

	=== "Before Sedona 1.3.1"

		```xml
		<!-- https://mvnrepository.com/artifact/org.datasyslab/sernetcdf -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>sernetcdf</artifactId>
		    <version>0.1.0</version>
		</dependency>
		```

## Use Sedona unshaded jars

!!!warning
	For Scala, Java, Python users, please use the following jars only if you satisfy these conditions: (1) you know how to exclude transient dependencies in a complex application. (2) your environment has internet access (3) you are using some sort of Maven package resolver, or pom.xml, or build.sbt. It usually directly takes an input like this `GroupID:ArtifactID:Version`. If you don't understand what we are talking about, the following jars are not for you.

Apache Sedona provides different packages for each supported version of Spark.

* For Spark 3.0 to 3.3, the artifacts to use should be `sedona-spark-3.0_2.12`.
* For Spark 3.4 or higher versions, please use the artifacts with Spark major.minor version in the artifact name. For example, for Spark 3.4, the artifacts to use should be `sedona-spark-3.4_2.12`.

If you are using the Scala 2.13 builds of Spark, please use the corresponding packages for Scala 2.13, which are suffixed by `_2.13`.

The optional GeoTools library is required if you want to use CRS transformation, ShapefileReader or GeoTiff reader. This wrapper library is a re-distribution of GeoTools official jars. The only purpose of this library is to bring GeoTools jars from OSGEO repository to Maven Central. This library is under GNU Lesser General Public License (LGPL) license, so we cannot package it in Sedona official release.

!!! abstract "Sedona with Apache Spark and Scala 2.12"

	=== "Spark 3.0 to 3.3 and Scala 2.12"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.0_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.4 and Scala 2.12"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.4_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.5 and Scala 2.12"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.5_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona with Apache Spark and Scala 2.13"

	=== "Spark 3.0+ and Scala 2.13"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.0_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.4 and Scala 2.13"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.4_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```
	=== "Spark 3.5 and Scala 2.13"
		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-spark-3.5_2.13</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

!!! abstract "Sedona with Apache Flink"

	=== "Flink 1.12+ and Scala 2.12"

		```xml
		<dependency>
		  <groupId>org.apache.sedona</groupId>
		  <artifactId>sedona-flink_2.12</artifactId>
		  <version>{{ sedona.current_version }}</version>
		</dependency>
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>geotools-wrapper</artifactId>
		    <version>{{ sedona.current_geotools }}</version>
		</dependency>
		```

Sedona Snowflake does not have an unshaded version.

### netCDF-Java 5.4.2

This is required only if you want to read HDF/NetCDF files using `RS_FromNetCDF`. Note that this JAR is not in Maven Central so you will need to add this repository to your pom.xml or build.sbt, or specify the URL in Spark Config `spark.jars.repositories` or spark-submit `--repositories` option.

Under BSD 3-clause (compatible with Apache 2.0 license)

!!! abstract "Add HDF/NetCDF dependency"

	=== "Sedona 1.3.1+"

		Add unidata repo to your pom.xml

		```
		<repositories>
		    <repository>
		        <id>unidata-all</id>
		        <name>Unidata All</name>
		        <url>https://artifacts.unidata.ucar.edu/repository/unidata-all/</url>
		    </repository>
		</repositories>
		```

		Then add cdm-core to your POM dependency.

		```xml
		<dependency>
		    <groupId>edu.ucar</groupId>
		    <artifactId>cdm-core</artifactId>
		    <version>5.4.2</version>
		</dependency>
		```

	=== "Before Sedona 1.3.1"

		```xml
		<!-- https://mvnrepository.com/artifact/org.datasyslab/sernetcdf -->
		<dependency>
		    <groupId>org.datasyslab</groupId>
		    <artifactId>sernetcdf</artifactId>
		    <version>0.1.0</version>
		</dependency>
		```

## SNAPSHOT versions

Sometimes Sedona has a SNAPSHOT version for the upcoming release. It follows the same naming conversion but has "SNAPSHOT" as suffix in the version. For example, `{{ sedona_create_release.current_snapshot }}`

In order to download SNAPSHOTs, you need to add the following repositories in your pom.xml or build.sbt

### build.sbt

resolvers +=
  "Apache Software Foundation Snapshots" at "https://repository.apache.org/content/groups/snapshots"

### pom.xml

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
