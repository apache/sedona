/*
 * FILE: adapterTestJava
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geosparksql;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

public class adapterTestJava
        implements Serializable
{
    protected static SparkConf conf;
    protected static JavaSparkContext sc;
    protected static SparkSession sparkSession;
    public static String resourceFolder = System.getProperty("user.dir") + "/src/test/resources/";
    public static String mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv";
    public static String mixedWkbGeometryInputLocation = resourceFolder + "county_small_wkb.tsv";
    public static String csvPointInputLocation = resourceFolder + "arealm.csv";
    public static String shapefileInputLocation = resourceFolder + "shapefiles/polygon";

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        conf = new SparkConf().setAppName("adapterTestJava").setMaster("local[2]");
        conf.set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        sparkSession = new SparkSession(sc.sc());
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext());
    }

    @Test
    public void testReadCsv()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(csvPointInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_PointFromText(inputtable._c0,\",\",\"mypoint\") as arealandmark from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show();
    }

    @Test
    public void testReadCsvUsingCoordinates()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show();
    }

    @Test
    public void testReadCsvWithIdUsingCoordinates()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show();
    }

    @Test
    public void testReadWkt()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show();
    }

    @Test
    public void testReadWktWithId()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0, inputtable._c3, inputtable._c5) as usacounty from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show();
    }

    @Test
    public void testReadWkb()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWkbGeometryInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromWKB(inputtable._c0) as usacounty from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show();
    }

    @Test
    public void testReadWkbWithId()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWkbGeometryInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromWKB(inputtable._c0, inputtable._c3, inputtable._c5) as usacounty from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show();
    }

    @Test
    public void testReadShapefileToDF()
    {
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), shapefileInputLocation);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show();
    }

    @Test
    public void testSpatialJoinToDataFrame()
            throws Exception
    {
        Dataset<Row> pointCsvDf = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation);
        pointCsvDf.createOrReplaceTempView("pointtable");
        Dataset<Row> pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable");
        SpatialRDD pointRDD = new SpatialRDD<Geometry>();
        pointRDD.rawSpatialRDD = Adapter.toJavaRdd(pointDf);
        pointRDD.analyze();

        Dataset<Row> polygonWktDf = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation);
        polygonWktDf.createOrReplaceTempView("polygontable");
        Dataset<Row> polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0, polygontable._c3, polygontable._c5) as usacounty from polygontable");
        SpatialRDD polygonRDD = new SpatialRDD<Geometry>();
        polygonRDD.rawSpatialRDD = Adapter.toJavaRdd(polygonDf);
        polygonRDD.analyze();

        pointRDD.spatialPartitioning(GridType.QUADTREE);
        polygonRDD.spatialPartitioning(pointRDD.getPartitioner());

        pointRDD.buildIndex(IndexType.QUADTREE, true);

        JavaPairRDD joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD, polygonRDD, true, true);

        Dataset joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession);

        joinResultDf.show();
    }

    @Test
    public void testDistanceJoinToDataFrame()
            throws Exception
    {
        Dataset<Row> pointCsvDf = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation);
        pointCsvDf.createOrReplaceTempView("pointtable");
        Dataset<Row> pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable");
        SpatialRDD pointRDD = new SpatialRDD<Geometry>();
        pointRDD.rawSpatialRDD = Adapter.toJavaRdd(pointDf);
        pointRDD.analyze();

        Dataset<Row> polygonWktDf = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation);
        polygonWktDf.createOrReplaceTempView("polygontable");
        Dataset<Row> polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0, polygontable._c3, polygontable._c5) as usacounty from polygontable");
        SpatialRDD polygonRDD = new SpatialRDD<Geometry>();
        polygonRDD.rawSpatialRDD = Adapter.toJavaRdd(polygonDf);
        polygonRDD.analyze();

        CircleRDD circleRDD = new CircleRDD(polygonRDD, 0.2);

        pointRDD.spatialPartitioning(GridType.QUADTREE);
        circleRDD.spatialPartitioning(pointRDD.getPartitioner());

        pointRDD.buildIndex(IndexType.QUADTREE, true);

        JavaPairRDD joinResultPairRDD = JoinQuery.DistanceJoinQueryFlat(pointRDD, circleRDD, true, true);

        Dataset joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession);

        joinResultDf.show();
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        GeoSparkSQLRegistrator.dropAll(sparkSession);
        sparkSession.stop();
    }
}
