package edu.utdallas.cg.spatial_rdd.core.tree;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public interface Tree<T> extends Serializable {

  boolean isLeaf();

  int getLeafId();

  Envelope getBoundaryEnvelope();

  void insert(Envelope envelope);

  void dropElements();

  List<T> findLeafNodes(Envelope envelope);

  void assignLeafIds();

  Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry geometry);

  List<Envelope> fetchLeafZones();
}
