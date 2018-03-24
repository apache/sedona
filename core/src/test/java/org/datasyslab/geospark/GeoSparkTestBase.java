package org.datasyslab.geospark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;

public class GeoSparkTestBase {
    protected static SparkConf conf;
    protected static JavaSparkContext sc;

    protected static void initialize(final String testSuiteName) {
        conf = new SparkConf().setAppName(testSuiteName).setMaster("local[2]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }
}

