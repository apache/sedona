package org.apache.sedona.core.partition.impl;

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.partition.SpatialPartitioner;
import org.apache.sedona.core.tree.KDB;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.Iterator;

public class KDBTreePartitioner extends SpatialPartitioner {
  private final KDB tree;

  public KDBTreePartitioner(KDB tree) {
    super(GridType.KDBTREE, tree.fetchLeafZones());
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
