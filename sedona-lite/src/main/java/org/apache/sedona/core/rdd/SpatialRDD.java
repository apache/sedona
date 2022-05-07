package org.apache.sedona.core.rdd;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.index.IndexBuilder;
import org.apache.sedona.core.partition.SpatialPartitioner;
import org.apache.sedona.core.partition.impl.KDBTreePartitioner;
import org.apache.sedona.core.spatial.StatCalculator;
import org.apache.sedona.core.tree.KDB;
import org.apache.sedona.core.utils.RDDSampleUtils;
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

  public JavaRDD<T> spatialPartitionedRDD;
  /** The raw org.apache.sedona.core.spatial RDD. */
  public JavaRDD<T> rawSpatialRDD;

  public JavaRDD<SpatialIndex> indexedRawRDD;

  public JavaRDD<SpatialIndex> indexedRDD;

  public List<String> fieldNames;

  private SpatialPartitioner partitioner;

  public long approximateTotalCount = -1;

  private int sampleNumber = -1;

  /** The boundary envelope. */
  public Envelope boundaryEnvelope = null;

  @SneakyThrows
  public boolean spatialPartitioning(GridType gridType) {
    int numPartitions = this.rawSpatialRDD.rdd().partitions().length;
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
      throw new Exception(
          "[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call analyze() first.");
    }
    if (this.approximateTotalCount == -1) {
      throw new Exception(
          "[AbstractSpatialRDD][spatialPartitioning] SpatialRDD total count is unkown. Please call analyze() first.");
    }

    // Calculate the number of samples we need to take.
    int sampleNumberOfRecords =
        RDDSampleUtils.getSampleNumbers(
            numPartitions, this.approximateTotalCount, this.sampleNumber);
    // Take Sample
    // RDD.takeSample implementation tends to scan the data multiple times to gather the exact
    // number of samples requested. Repeated scans increase the latency of the join. This increase
    // is significant for large datasets.
    // See
    // https://github.com/apache/spark/blob/412b0e8969215411b97efd3d0984dc6cac5d31e0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L508
    // Here, we choose to get samples faster over getting exactly specified number of samples.
    final double fraction =
        SamplingUtils.computeFractionForSampleSize(
            sampleNumberOfRecords, approximateTotalCount, false);
    List<Envelope> samples =
        this.rawSpatialRDD
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
      case KDBTREE:
        {
          final KDB tree = new KDB(samples.size() / numPartitions, numPartitions, paddedBoundary);
          for (final Envelope sample : samples) {
            tree.insert(sample);
          }
          tree.assignLeafIds();

          partitioner = new KDBTreePartitioner(tree);
          break;
        }
      default:
        throw new Exception(
            "[AbstractSpatialRDD][spatialPartitioning] Unsupported org.apache.sedona.core.spatial partitioning method. "
                + "The following partitioning methods are not longer supported: R-Tree, Hilbert curve, Voronoi");
    }
  }

  private JavaRDD<T> partition(final SpatialPartitioner partitioner) {
    return this.rawSpatialRDD
        .flatMapToPair(
            (PairFlatMapFunction<T, Integer, T>)
                spatialObject -> partitioner.placeObject(spatialObject))
        .partitionBy(partitioner)
        .mapPartitions(
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

  public void setRawSpatialRDD(JavaRDD<T> rawSpatialRDD) {
    this.rawSpatialRDD = rawSpatialRDD;
  }

  public boolean analyze(Envelope datasetBoundary, Integer approximateTotalCount) {
    this.boundaryEnvelope = datasetBoundary;
    this.approximateTotalCount = approximateTotalCount;
    return true;
  }

  public boolean analyze() {
    final Function2 combOp =
        (Function2<StatCalculator, StatCalculator, StatCalculator>)
            (agg1, agg2) -> StatCalculator.combine(agg1, agg2);

    final Function2 seqOp =
        (Function2<StatCalculator, Geometry, StatCalculator>)
            (agg, object) -> StatCalculator.add(agg, object);

    StatCalculator agg = (StatCalculator) this.rawSpatialRDD.aggregate(null, seqOp, combOp);
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
      // This org.apache.sedona.core.index is built on top of unpartitioned SRDD
      this.indexedRawRDD = this.rawSpatialRDD.mapPartitions(new IndexBuilder(indexType));
    } else {
      if (this.spatialPartitionedRDD == null) {
        throw new Exception(
            "[AbstractSpatialRDD][buildIndex] spatialPartitionedRDD is null. Please do org.apache.sedona.core.spatial partitioning before build org.apache.sedona.core.index.");
      }
      this.indexedRDD = this.spatialPartitionedRDD.mapPartitions(new IndexBuilder(indexType));
    }
  }

  public JavaRDD<T> getRawSpatialRDD() {
    return rawSpatialRDD;
  }
}
