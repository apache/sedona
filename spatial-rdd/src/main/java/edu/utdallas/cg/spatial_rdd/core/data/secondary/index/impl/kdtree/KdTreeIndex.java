package edu.utdallas.cg.spatial_rdd.core.data.secondary.index.impl.kdtree;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.kdtree.KdNode;
import org.locationtech.jts.index.kdtree.KdTree;

public class KdTreeIndex extends KdTree implements SpatialIndex {

  public static final double NULL_ORDINATE = Double.NaN;

  @Override
  public void insert(Envelope itemEnv, Object item) {
    Coordinate coordinate = new Coordinate(itemEnv.getMinX(), itemEnv.getMinY(), NULL_ORDINATE);
    insert(coordinate, item);
  }

  @Override
  public List query(Envelope searchEnv) {
    List nodes = super.query(searchEnv);
    List candidatePoints = new ArrayList<>();
    for (Object o : nodes) {
      candidatePoints.add(((KdNode) o).getData());
    }
    return candidatePoints;
  }

  @Override
  public void query(Envelope searchEnv, ItemVisitor visitor) {
    // implementation not required
  }

  @Override
  public boolean remove(Envelope itemEnv, Object item) {
    // implementation not required
    return false;
  }
}
