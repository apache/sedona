# Apache Spark 2.X versions
Please add the following dependencies into your POM.xml or build.sbt
## GeoSpark-Core
```
groupId: org.datasyslab
artifactId: geospark
version: 1.3.1
```
## GeoSpark-SQL
### For SparkSQL-2.3
```
groupId: org.datasyslab
artifactId: geospark-sql_2.3
version: 1.3.1
```
### For SparkSQL-2.2
```
groupId: org.datasyslab
artifactId: geospark-sql_2.2
version: 1.3.1
```
### For SparkSQL-2.1
```
groupId: org.datasyslab
artifactId: geospark-sql_2.1
version: 1.3.1
```
## GeoSpark-Viz 1.2.0 and later
### For SparkSQL-2.3
```
groupId: org.datasyslab
artifactId: geospark-viz_2.3
version: 1.3.1
```
### For SparkSQL-2.2
```
groupId: org.datasyslab
artifactId: geospark-viz_2.2
version: 1.3.1
```
### For SparkSQL-2.1
```
groupId: org.datasyslab
artifactId: geospark-viz_2.1
version: 1.3.1
```

## GeoSpark-Viz 1.1.3 and earlier
```
groupId: org.datasyslab
artifactId: geospark-viz
version: 1.1.3
```

---

## Apache Spark 1.X versions
Please add the following dependencies into your POM.xml or build.sbt
### GeoSpark-Core
```
groupId: org.datasyslab
artifactId: geospark
version: 0.8.2-spark-1.x
```
### GeoSpark-Viz
```
groupId: org.datasyslab
artifactId: babylon
version: 0.2.1-spark-1.x
```

---
## SNAPSHOT versions
Sometimes GeoSpark has a SNAPSHOT version for the upcoming release. "SNAPSHOT" is uppercase.
```
groupId: org.datasyslab
artifactId: geospark
version: 1.3.2-SNAPSHOT
```

```
groupId: org.datasyslab
artifactId: geospark-sql_2.3
version: 1.3.2-SNAPSHOT
```

```
groupId: org.datasyslab
artifactId: geospark-viz
version: 1.3.2-SNAPSHOT
```

In order to download SNAPSHOTs, you need to add the following repositories in your POM.XML or build.sbt
### build.sbt
resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
### POM.XML
    <profiles>
        <profile>
            <id>allow-snapshots</id>
            <activation><activeByDefault>true</activeByDefault></activation>
            <repositories>
                <repository>
                    <id>snapshots-repo</id>
                    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
                    <releases><enabled>false</enabled></releases>
                    <snapshots><enabled>true</enabled></snapshots>
                </repository>
            </repositories>
        </profile>
    </profiles>
