package edu.utdallas.cg.spatial_rdd.core.rdd.primary.partition.impl;

import edu.utdallas.cg.spatial_rdd.core.rdd.primary.partition.SpatialPartitioner;
import edu.utdallas.cg.spatial_rdd.enums.GridType;
import edu.utdallas.cg.spatial_rdd.core.tree.kd.KdTree;

import java.util.Iterator;

import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public class KdTreePartitioner extends SpatialPartitioner {
  private final KdTree tree;

  public KdTreePartitioner(KdTree tree) {
    super(GridType.KD_TREE, tree.fetchLeafZones());
    this.tree = tree;
    this.tree.dropElements();
  }

  @Override
  public int numPartitions() {
    return grids.size();
  }

  @Override
  public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry spatialObject) throws Exception {
    return tree.placeObject(spatialObject);
  }
}
