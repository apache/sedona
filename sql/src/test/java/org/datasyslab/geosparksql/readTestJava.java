package org.datasyslab.geosparksql;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.UDF.UdfRegistrator;
import org.datasyslab.geosparksql.utils.Adapter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

public class readTestJava implements Serializable {
    protected static SparkConf conf;
    protected static JavaSparkContext sc;
    protected static SparkSession sparkSession;
    public static String resourceFolder = System.getProperty("user.dir")+"/src/test/resources/";
    public static String mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv";
    public static String csvPointInputLocation = resourceFolder + "arealm.csv";
    public static String shapefileInputLocation = resourceFolder + "shapefiles/polygon";

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        conf = new SparkConf().setAppName("readTestJava").setMaster("local[2]");
        conf.set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        sparkSession = new SparkSession(sc.sc());
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    @Test
    public void testReadCsv()
    {
        UdfRegistrator udfRegistrator = new UdfRegistrator();
        udfRegistrator.registerAll(sparkSession);
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter","\t").option("header","false").load(csvPointInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromTextToType(inputtable._c0,\",\",\"point\") as arealandmark from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD,sparkSession).show();
    }

    @Test
    public void testReadCsvUsingCoordinates()
    {
        UdfRegistrator udfRegistrator = new UdfRegistrator();
        udfRegistrator.registerAll(sparkSession);
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_Point(inputtable._c0,inputtable._c1) as arealandmark from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD,sparkSession).show();
    }

    @Test
    public void testReadCsvWithIdUsingCoordinates()
    {
        UdfRegistrator udfRegistrator = new UdfRegistrator();
        udfRegistrator.registerAll(sparkSession);
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_PointWithId(inputtable._c0,inputtable._c1,inputtable._c0) as arealandmark from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD,sparkSession).show();
    }

    @Test
    public void testReadWkt()
    {
        UdfRegistrator udfRegistrator = new UdfRegistrator();
        udfRegistrator.registerAll(sparkSession);
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromText(inputtable._c0,\"wkt\") as usacounty from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD,sparkSession).show();
    }

    @Test
    public void testReadWktWithId()
    {
        UdfRegistrator udfRegistrator = new UdfRegistrator();
        udfRegistrator.registerAll(sparkSession);
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation);
        df.show();
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromTextWithId(inputtable._c0,\"wkt\", concat(inputtable._c3,'\t',inputtable._c5)) as usacounty from inputtable");
        spatialDf.show();
        spatialDf.printSchema();
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = Adapter.toJavaRdd(spatialDf);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD,sparkSession).show();
    }

    @Test
    public void testReadShapefileToDF()
    {
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),shapefileInputLocation);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD,sparkSession).show();
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sparkSession.stop();
    }
}
