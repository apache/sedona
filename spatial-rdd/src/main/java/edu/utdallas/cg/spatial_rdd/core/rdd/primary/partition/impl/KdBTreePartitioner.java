package edu.utdallas.cg.spatial_rdd.core.rdd.primary.partition.impl;

import edu.utdallas.cg.spatial_rdd.core.rdd.primary.partition.SpatialPartitioner;
import edu.utdallas.cg.spatial_rdd.enums.GridType;
import edu.utdallas.cg.spatial_rdd.core.tree.kdb.KdBTree;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.Iterator;

public class KdBTreePartitioner extends SpatialPartitioner {
  private final KdBTree tree;

  public KdBTreePartitioner(KdBTree tree) {
    super(GridType.KDB_TREE, tree.fetchLeafZones());
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
