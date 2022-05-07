package org.apache.sedona.core.partition;

import org.apache.sedona.core.enums.GridType;
import org.apache.spark.Partitioner;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public abstract class SpatialPartitioner extends Partitioner implements Serializable {

  protected final GridType gridType;
  protected final List<Envelope> grids;

  protected SpatialPartitioner(GridType gridType, List<Envelope> grids) {
    this.gridType = gridType;
    this.grids = Objects.requireNonNull(grids, "grids");
  }

  /**
   * Given a geometry, returns a list of partitions it overlaps.
   *
   * <p>For points, returns exactly one org.apache.sedona.core.partition as long as grid type is non-overlapping. For other
   * geometry types or for overlapping grid types, may return multiple partitions.
   */
  public abstract <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
      throws Exception;

  @Override
  public int getPartition(Object key) {
    return (int) key;
  }
}
