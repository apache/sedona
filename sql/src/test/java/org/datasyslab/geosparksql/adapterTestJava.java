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
import org.datasyslab.geosparksql.UDF.UdfRegistrator;
import org.datasyslab.geosparksql.UDT.UdtRegistrator;
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
        UdtRegistrator.registerAll();
        UdfRegistrator.registerAll(sparkSession.sqlContext());
    }

    @Test
    public void testReadCsv()
    {
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

    @Test
    public void testSpatialJoinToDataFrame() throws Exception {
        Dataset<Row> pointCsvDf = sparkSession.read().format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation);
        pointCsvDf.createOrReplaceTempView("pointtable");
        Dataset<Row> pointDf = sparkSession.sql("select ST_PointWithId(pointtable._c0,pointtable._c1,\"mypointid\") as arealandmark from pointtable");
        SpatialRDD pointRDD = new SpatialRDD<Geometry>();
        pointRDD.rawSpatialRDD = Adapter.toJavaRdd(pointDf);
        pointRDD.analyze();

        Dataset<Row> polygonWktDf = sparkSession.read().format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation);
        polygonWktDf.createOrReplaceTempView("polygontable");
        Dataset<Row> polygonDf = sparkSession.sql("select ST_GeomFromTextWithId(polygontable._c0,\"wkt\", concat(polygontable._c3,'\t',polygontable._c5)) as usacounty from polygontable");
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
    public void testDistanceJoinToDataFrame() throws Exception {
        Dataset<Row> pointCsvDf = sparkSession.read().format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation);
        pointCsvDf.createOrReplaceTempView("pointtable");
        Dataset<Row> pointDf = sparkSession.sql("select ST_PointWithId(pointtable._c0,pointtable._c1,\"mypointid\") as arealandmark from pointtable");
        SpatialRDD pointRDD = new SpatialRDD<Geometry>();
        pointRDD.rawSpatialRDD = Adapter.toJavaRdd(pointDf);
        pointRDD.analyze();

        Dataset<Row> polygonWktDf = sparkSession.read().format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation);
        polygonWktDf.createOrReplaceTempView("polygontable");
        Dataset<Row> polygonDf = sparkSession.sql("select ST_GeomFromTextWithId(polygontable._c0,\"wkt\", concat(polygontable._c3,'\t',polygontable._c5)) as usacounty from polygontable");
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
    public static void TearDown() {
        UdfRegistrator.dropAll();
        sparkSession.stop();
    }
}
