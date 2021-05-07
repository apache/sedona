package org.apache.sedona.sql;


import org.apache.hadoop.fs.FileUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;
import org.apache.spark.sql.Dataset;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;


public class rasterTestJava   {

    public static String resourcefolder =  System.getProperty("user.dir") + "/../core/src/test/resources/";
    public static String rasterdatalocation = resourcefolder + "raster/image.tif";
    protected static SparkConf conf;
    protected static JavaSparkContext sc;
    protected static SparkSession sparkSession;
    protected static String hdfsURI;
    protected static String rasterfileHDFSpath;
    private static  FileSystem fs;
    private static MiniDFSCluster hdfsCluster;
    private static String localcsvPath;
    private static String hdfscsvpath;


    @BeforeClass
    public static void onceExecutedBeforeAll() throws IOException {
        // Set up spark configurations
        conf = new SparkConf().setAppName("rasterTestJava").setMaster("local[2]");
        conf.set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());
        sc = new JavaSparkContext(conf);
        sparkSession = new SparkSession(sc.sc());
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        SedonaSQLRegistrator.registerAll(sparkSession.sqlContext());

        // Set up HDFS mini-cluster configurations
        File baseDir = new File("target/hdfs").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        HdfsConfiguration hdfsConf = new HdfsConfiguration();
        hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdfsConf);
        hdfsCluster = builder.build();
        fs = FileSystem.get(hdfsConf);
        hdfsURI = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort() + "/";
        localcsvPath = baseDir.getAbsolutePath() + "/train.csv";
        System.out.println(localcsvPath);
        hdfscsvpath = hdfsURI + "train.csv";


    }

    @AfterClass
    public static void TearDown() throws IOException {

        SedonaSQLRegistrator.dropAll(sparkSession);
        sparkSession.stop();
        hdfsCluster.shutdown();
        fs.close();
    }

    // Testing constructor ST_GeomFromRaster which fetches geometrical extent for an image
    @Test
    public void geomfromRaster() throws IOException {


        rasterfileHDFSpath = hdfsURI + "image.tif";
        fs.copyFromLocalFile(new Path(rasterdatalocation), new Path(rasterfileHDFSpath));
        createFileLocal();
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(localcsvPath);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromRaster(inputtable._c0) as countyshape from inputtable");
        spatialDf.show();
        assert(spatialDf.count()==2);

    }

    // Testing ST_DataframeFromRaster constructor which converts spark dataframe into geotiff dataframe in Apache Sedona
    @Test
    public void sedonadataframeLoader() throws IOException {

        rasterfileHDFSpath = hdfsURI + "image.tif";
        fs.copyFromLocalFile(new Path(rasterdatalocation), new Path(rasterfileHDFSpath));
        createFileLocal();
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(localcsvPath);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_DataframeFromRaster(inputtable._c0, 4) as rasterstruct from inputtable");
        spatialDf.show(false);
        spatialDf.printSchema();
        spatialDf.createOrReplaceTempView("sedonaframe");
        Dataset<Row> sedonaDF = sparkSession.sql("select rasterstruct.Polygon as geom, rasterstruct.band1 as Band_1, rasterstruct.band2 as Band_2, rasterstruct.band3 as Band_3, rasterstruct.band4 as Band_4 from sedonaframe");
        sedonaDF.show();
        assert(sedonaDF.count()==2 && sedonaDF.columns().length==5);

    }

    // Create a CSV file on local with image URL containing HDFS paths(paths can either be on HDFS or S3 bucket)
    private void createFileLocal() throws IllegalArgumentException, IOException {
        List<List<String>> rows = Arrays.asList(
                Arrays.asList(hdfsURI + "image.tif"),
                Arrays.asList(hdfsURI + "image.tif")
        );

        FileWriter csvWriter = new FileWriter(localcsvPath);
        for (List<String> rowData : rows) {
            csvWriter.append(String.join(",", rowData));
            csvWriter.append("\n");
        }
        csvWriter.flush();
        csvWriter.close();

    }

}