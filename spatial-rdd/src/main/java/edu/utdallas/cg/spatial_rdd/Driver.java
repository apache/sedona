package edu.utdallas.cg.spatial_rdd;

import edu.utdallas.cg.spatial_rdd.core.query.range.RangeQuery;
import edu.utdallas.cg.spatial_rdd.core.rdd.SpatialRDD;
import edu.utdallas.cg.spatial_rdd.enums.GridType;
import edu.utdallas.cg.spatial_rdd.enums.IndexType;
import edu.utdallas.cg.spatial_rdd.file.io.impl.GeoJsonRddReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Envelope;

public class Driver {

  public static void main(String[] args) throws Exception {

    // Config
    SparkConf conf = new SparkConf().setAppName("spatial-rdd").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // 1. Load
    String inputPath = Driver.class.getClassLoader().getResource("geojson_points.json").getPath();
    SpatialRDD spatialRDD = GeoJsonRddReader.readToGeometryRDD(sc, inputPath);

    // 2.a init params
    spatialRDD.analyze();

    // 2.b Spatial Partition : Sample + Partition
    spatialRDD.spatialPartitioning(GridType.KD_TREE);
    System.out.println("HashRDD Count:" + spatialRDD.rawRdd.partitions().size());
    System.out.println("SpatialRDD Count:" + spatialRDD.spatialPartitionedRDD.partitions().size());

    // 3. Query
    Envelope queryEnvelope = new Envelope(-80.01, -75.01, 40, 40.4);

    // 3.a Query: without Indexing
    long resultSize = RangeQuery.spatialRangeQuery(spatialRDD, queryEnvelope, false, false).count();
    System.out.println("Query without indexing : " + resultSize);

    // 3.b Query: with indexing
    spatialRDD.buildIndex(IndexType.KDTREE, true);
    resultSize = RangeQuery.spatialRangeQuery(spatialRDD, queryEnvelope, false, true).count();
    System.out.println("Query with indexing : " + resultSize);
  }
}
