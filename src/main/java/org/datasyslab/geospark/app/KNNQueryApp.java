package org.datasyslab.geospark.app;

/* This is an example of GeoSpark range query
 * Introduction: In spatial KNN query, you provide a pointset (in spatialRDD).You also need to provide one poin
 * and a point area, running KNN query will return K nearest point to the original point.
 * Arguments: 
 * 1. Spark master IP;
 * 2. Input 1 location;
 * 3. Input 1 partitions number;
 * 4. Input 1 spatial info starting column;
 * 5. Format name "tsv || csv || wkt";
 * Result is saved at "hdfs://"+IP+":54310/test/tempResult.txt";
 */
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import org.datasyslab.geospark.partition.*;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.gemotryObjects.*;
import org.datasyslab.geospark.boundryFilter.*;
import org.datasyslab.geospark.spatialRDD.*;
import com.vividsolutions.jts.geom.Envelope;

public class KNNQueryApp {
    public static void main(String[] args) throws IOException {
        String IP=args[0];
        String set1="hdfs://"+IP+":54310/"+args[1];
        Integer partitions1=Integer.parseInt(args[2]);
        Integer offset1=Integer.parseInt(args[3]);
        String Splitter1=args[4];
        String outputlocation="hdfs://"+IP+":54310/test/tempResult.txt";
        URI uri=URI.create(outputlocation);
        Path pathhadoop=new Path(uri);
        SparkConf conf=new SparkConf().setAppName("Geospark_KNN");//.setMaster("spark://"+IP+":7077");//.set("spark.cores.max", "136").set("spark.executor.memory", "50g").set("spark.executor.cores", "8").set("spark.local.ip", IP).set("spark.driver.memory", "13g").set("spark.driver.cores", "8").set("spark.driver.host", IP).set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/").set("spark.history.retainedApplications", "1000");//.set("spark.reducer.maxSizeInFlight","1000m" );//.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(kryo).set("spark.shuffle.memoryFraction", "0.2").set("spark.storage.memoryFraction", "0.6");//.set("spark.akka.failuredetector.threshold", "300000").set("spark.akka.threads", "300").set("spark.akka.timeout", "30000").set("spark.akka.askTimeout", "300").set("spark.storage.blockManagerTimeoutIntervalMs", "180000").set("spark.network.timeout", "10000").set("spark.blockManagerHeartBeatMs", "80000");
        JavaSparkContext spark=new JavaSparkContext(conf);
        Configuration confhadoop=spark.hadoopConfiguration();
        confhadoop.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        confhadoop.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        FileSystem filehadoop =FileSystem.get(uri, confhadoop);
        filehadoop.delete(pathhadoop, true);
        System.out.println("Old output file has been deleted!");
        //todo: set spark broadcast variable;
        GeometryFactory fact = new GeometryFactory();
        Coordinate coordinate = new Coordinate(0.0, 0.0);
        Point point=fact.createPoint(coordinate);

        //spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");

        PointRDD pointRDD=new PointRDD(spark,set1,offset1,Splitter1,partitions1);
        List<Point> reslist = KNNQuery.SpatialKnnQuery(pointRDD, spark.broadcast(point), 10);
        PointRDD rangeQueryRDD= new PointRDD(spark.parallelize(reslist));
        rangeQueryRDD.getPointRDD().saveAsTextFile(outputlocation);
        spark.stop();
    }
}
