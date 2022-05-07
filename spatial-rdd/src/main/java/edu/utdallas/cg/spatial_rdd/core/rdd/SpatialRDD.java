package edu.utdallas.cg.spatial_rdd.core.rdd;

import edu.utdallas.cg.spatial_rdd.core.rdd.primary.partition.SpatialPartitioner;
import edu.utdallas.cg.spatial_rdd.enums.GridType;
import edu.utdallas.cg.spatial_rdd.enums.IndexType;
import edu.utdallas.cg.spatial_rdd.core.rdd.secondary.index.IndexBuilder;
import edu.utdallas.cg.spatial_rdd.core.rdd.primary.partition.impl.KdBTreePartitioner;
import edu.utdallas.cg.spatial_rdd.core.rdd.primary.partition.impl.KdTreePartitioner;
import edu.utdallas.cg.spatial_rdd.core.approximation.StatCalculator;
import edu.utdallas.cg.spatial_rdd.core.tree.kd.KdTree;
import edu.utdallas.cg.spatial_rdd.core.tree.kdb.KdBTree;
import edu.utdallas.cg.spatial_rdd.core.approximation.RddSamplingUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.random.SamplingUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class SpatialRDD<T extends Geometry> implements Serializable {

  public JavaRDD<T> rawRdd;
  public JavaRDD<T> spatialPartitionedRDD;
  public JavaRDD<SpatialIndex> indexedRawRDD;
  public JavaRDD<SpatialIndex> indexedSpatialPartitionedRDD;

  public List<String> fieldNames;

  private SpatialPartitioner partitioner;

  public long approximateTotalCount = -1;

  /** The boundary envelope. */
  public Envelope boundaryEnvelope = null;

  @SneakyThrows
  public boolean spatialPartitioning(GridType gridType) {
    int numPartitions = this.rawRdd.rdd().partitions().length;
    spatialPartitioning(gridType, numPartitions);
    return true;
  }

  public void spatialPartitioning(GridType gridType, int numPartitions) throws Exception {
    calculatePartitioner(gridType, numPartitions);
    this.spatialPartitionedRDD = partition(partitioner);
  }

  public void calculatePartitioner(GridType gridType, int numPartitions) throws Exception {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Number of partitions must be >= 0");
    }

    if (this.boundaryEnvelope == null) {
      throw new Exception("SpatialRDD boundary is null. Please call analyze() first.");
    }

    if (this.approximateTotalCount == -1) {
      throw new Exception("SpatialRDD total count is unknown. Please call analyze() first.");
    }

    // Calculate the number of samples we need to take.
    int sampleNumberOfRecords =
        RddSamplingUtils.getSampleNumbers(numPartitions, this.approximateTotalCount);

    final double fraction =
        SamplingUtils.computeFractionForSampleSize(
            sampleNumberOfRecords, approximateTotalCount, false);

    List<Envelope> samples =
        this.rawRdd
            .sample(false, fraction)
            .map((Function<T, Envelope>) geometry -> geometry.getEnvelopeInternal())
            .collect();

    log.info("Collected " + samples.size() + " samples");

    // Add some padding at the top and right of the boundaryEnvelope to make
    // sure all geometries lie within the half-open rectangle.
    final Envelope paddedBoundary =
        new Envelope(
            boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
            boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

    switch (gridType) {
      case KDB_TREE:
        {
          final KdBTree tree =
              new KdBTree(samples.size() / numPartitions, numPartitions, paddedBoundary);
          for (final Envelope sample : samples) {
            tree.insert(sample);
          }
          tree.assignLeafIds();

          partitioner = new KdBTreePartitioner(tree);
          break;
        }

      case KD_TREE:
        {
          final KdTree tree = new KdTree(paddedBoundary);
          for (final Envelope sample : samples) {
            tree.insert(sample);
          }
          tree.assignLeafIds();

          partitioner = new KdTreePartitioner(tree);
          break;
        }
      default:
        throw new Exception("Unsupported spatial partitioning. We only support, KDTree & KDBTree ");
    }
  }

  private JavaRDD<T> partition(final SpatialPartitioner partitioner) {

    JavaPairRDD<Integer, T> leafIdAndGeometryPairRdd =
        this.rawRdd.flatMapToPair((PairFlatMapFunction<T, Integer, T>) partitioner::placeObject);

    JavaPairRDD<Integer, T> spatialPartitionedRdd =
        leafIdAndGeometryPairRdd.partitionBy(partitioner);

    return spatialPartitionedRdd.mapPartitions(
        (FlatMapFunction<Iterator<Tuple2<Integer, T>>, T>)
            tuple2Iterator ->
                new Iterator<T>() {
                  @Override
                  public boolean hasNext() {
                    return tuple2Iterator.hasNext();
                  }

                  @Override
                  public T next() {
                    return tuple2Iterator.next()._2();
                  }

                  @Override
                  public void remove() {
                    throw new UnsupportedOperationException();
                  }
                },
        true);
  }

  public boolean analyze() {
    final Function2 combOp =
        (Function2<StatCalculator, StatCalculator, StatCalculator>)
            (agg1, agg2) -> StatCalculator.combine(agg1, agg2);

    final Function2 seqOp =
        (Function2<StatCalculator, Geometry, StatCalculator>)
            (agg, object) -> StatCalculator.add(agg, object);

    StatCalculator agg = (StatCalculator) this.rawRdd.aggregate(null, seqOp, combOp);
    if (agg != null) {
      this.boundaryEnvelope = agg.getBoundary();
      this.approximateTotalCount = agg.getCount();
    } else {
      this.boundaryEnvelope = null;
      this.approximateTotalCount = 0;
    }
    return true;
  }

  public void buildIndex(final IndexType indexType, boolean buildIndexOnSpatialPartitionedRDD)
      throws Exception {
    if (!buildIndexOnSpatialPartitionedRDD) {
      // This index is built on top of hash partitioned RDD
      this.indexedRawRDD = this.rawRdd.mapPartitions(new IndexBuilder(indexType));
    } else {

      // This index is built on top of spatially partitioned RDD
      if (this.spatialPartitionedRDD == null)
        throw new Exception("spatialPartitionedRDD is null. run spatialPartitioning");

      this.indexedSpatialPartitionedRDD =
          this.spatialPartitionedRDD.mapPartitions(new IndexBuilder(indexType));
    }
  }

  public JavaRDD<T> getRawRdd() {
    return rawRdd;
  }
}
