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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import java.nio.file.Files;

public class rasterTestJava   {
    protected static SparkConf conf;
    protected static JavaSparkContext sc;
    protected static SparkSession sparkSession;
    protected static String filePath;
    protected static String hdfsURI;
    protected static String rasterfileHDFSpath;
    private static  FileSystem fs;
    private static MiniDFSCluster hdfsCluster;
    private static String csvPath;
    public static String resourcefolder =  System.getProperty("user.dir") + "/../core/src/test/resources/";
    public static String rasterdatalocation = resourcefolder + "raster/image.tif";

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


        File baseDir = new File("target/hdfs").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        HdfsConfiguration hdfsConf = new HdfsConfiguration();
        hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdfsConf);
        hdfsCluster = builder.build();
        fs = FileSystem.get(hdfsConf);
        hdfsURI = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort() + "/";
        csvPath = baseDir.getAbsolutePath() + "/train.csv";
        System.out.println(csvPath);

    }
    @AfterClass
    public static void TearDown() throws IOException {

        SedonaSQLRegistrator.dropAll(sparkSession);
        sparkSession.stop();
        hdfsCluster.shutdown();
        fs.close();


    }
    @Test
    public void readImagesFromHDFS() throws IOException {


        rasterfileHDFSpath = hdfsURI + "image.tif";
        fs.copyFromLocalFile(new Path(rasterdatalocation), new Path(rasterfileHDFSpath));
        createFileHDFS();
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPath);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromRaster(inputtable._c0) as countyshape from inputtable");
        spatialDf.show();
        assert(spatialDf.count()==2);

    }

    @Test
    public void readImagesFromLocal() throws IOException {

        

    }


    private void createFileHDFS() throws IllegalArgumentException, IOException {
        List<List<String>> rows = Arrays.asList(
                Arrays.asList(hdfsURI + "image.tif"),
                Arrays.asList(hdfsURI + "image.tif")
        );

        FileWriter csvWriter = new FileWriter(csvPath);
        for (List<String> rowData : rows) {
            csvWriter.append(String.join(",", rowData));
            csvWriter.append("\n");
        }
        csvWriter.flush();
        csvWriter.close();

    }



}