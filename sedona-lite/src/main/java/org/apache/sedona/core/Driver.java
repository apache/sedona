package org.apache.sedona.core;

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.file.io.impl.GeoJsonReader;
import org.apache.sedona.core.operators.RangeQuery;
import org.apache.sedona.core.rdd.SpatialRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Envelope;

public class Driver {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf().setAppName("sedona-lite").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // 1 Load
    String inputPath = Driver.class.getClassLoader().getResource("testPolygon.json").getPath();
    SpatialRDD spatialRDD = GeoJsonReader.readToGeometryRDD(sc, inputPath);

    // 2.a init parameters
    spatialRDD.analyze();

    // 2.b Spatial Partition : Sample + Partition
    spatialRDD.spatialPartitioning(GridType.KDBTREE);
    System.out.println("HashRDD Count:" + spatialRDD.rawSpatialRDD.partitions().size());
    System.out.println("SpatialRDD Count:" + spatialRDD.spatialPartitionedRDD.partitions().size());

    // 3. Query
    Envelope queryEnvelope = new Envelope(-90.01, -80.01, 30.01, 40.01);

    // 3.a Query: without Indexing
    long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count();
    System.out.println(resultSize);

    // 3.b Query: with indexing
    spatialRDD.buildIndex(IndexType.RTREE, false);
    resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).count();
    System.out.println(resultSize);
  }
}
