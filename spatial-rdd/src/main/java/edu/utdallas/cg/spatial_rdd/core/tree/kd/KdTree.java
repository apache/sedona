package edu.utdallas.cg.spatial_rdd.core.tree.kd;

import edu.utdallas.cg.spatial_rdd.core.tree.Tree;
import edu.utdallas.cg.spatial_rdd.core.tree.Visitor;
import edu.utdallas.cg.spatial_rdd.utils.HalfOpenRectangle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

public class KdTree implements Tree<KdTree> {

  private final Envelope boundaryEnvelope;
  private final int dimension;
  private final int depth;
  private Envelope nodeKey;
  private KdTree left = null;
  private KdTree right = null;

  private int leafId = 0;

  public KdTree(Envelope boundaryEnvelope) {
    this(2, 0, boundaryEnvelope);
  }

  private KdTree(int dimensions, int depth, Envelope boundaryEnvelope) {
    this.boundaryEnvelope = boundaryEnvelope;
    this.depth = depth;
    this.dimension = dimensions;
  }


  @Override
  public boolean isLeaf() {
    return left == null && right == null;
  }

  @Override
  public int getLeafId() {
    if (!isLeaf()) {
      throw new IllegalStateException();
    }

    return leafId;
  }

  @Override
  public Envelope getBoundaryEnvelope() {
    return boundaryEnvelope;
  }

  @Override
  public void insert(Envelope envelope) {
    if (nodeKey == null) {
      nodeKey = envelope;
    } else {
      if (left == null && right == null) {
        boolean splitX = depth % dimension == 0;
        boolean ok = split(splitX);
        if (!ok) {
          ok = split(!splitX);
        }

        if (!ok) {
          nodeKey = envelope;
          return;
        }
      }

      if (left != null && left.boundaryEnvelope.contains(envelope.getMinX(), envelope.getMaxY())) {
        left.insert(envelope);
      } else if (right != null && right.boundaryEnvelope.contains(envelope.getMinX(), envelope.getMaxY())) {
        right.insert(envelope);
      }
    }
  }

  private boolean split(boolean splitX) {
    final Envelope[] splits;
    final Splitter splitter;
    double splitVal = getKeyLowerBound(splitX);
    if (splitVal > getExtendLowerBound(splitX) && splitVal < getExtendUpperBound(splitX)) {
      splits = splitAt(splitX, splitVal);
      splitter = getSplitter(splitX, splitVal);
    } else {
      // Too many objects are crowded at the edge of the extent. Can't split.
      return false;
    }

    left = new KdTree(dimension, depth + 1, splits[0]);
    right = new KdTree(dimension, depth + 1, splits[1]);

    // Move items
    pushNodeKeyDown(splitter);
    return true;
  }

  private void pushNodeKeyDown(Splitter splitter) {
    if (splitter.split(nodeKey)) {
      left.insert(nodeKey);
    } else {
      right.insert(nodeKey);
    }
  }

  private Envelope[] splitAtX(Envelope envelope, double x) {
    Envelope[] splits = new Envelope[2];
    splits[0] = new Envelope(envelope.getMinX(), x, envelope.getMinY(), envelope.getMaxY());
    splits[1] = new Envelope(x, envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
    return splits;
  }

  private Envelope[] splitAtY(Envelope envelope, double y) {
    Envelope[] splits = new Envelope[2];
    splits[0] = new Envelope(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), y);
    splits[1] = new Envelope(envelope.getMinX(), envelope.getMaxX(), y, envelope.getMaxY());
    return splits;
  }

  @Override
  public void dropElements() {
    traverse(
        tree -> {
          tree.nodeKey = null;
          return true;
        });
  }

  @Override
  public List<KdTree> findLeafNodes(final Envelope envelope) {
    final List<KdTree> matches = new ArrayList<>();
    traverse(
        tree -> {
          if (!disjoint(tree.getBoundaryEnvelope(), envelope)) {
            if (tree.isLeaf()) {
              matches.add(tree);
            }
            return true;
          } else {
            return false;
          }
        });

    return matches;
  }

  @Override
  public void assignLeafIds() {
    traverse(
        new Visitor<>() {
          int id = 0;

          @Override
          public boolean visit(KdTree tree) {
            if (tree.isLeaf()) {
              tree.leafId = id;
              id++;
            }
            return true;
          }
        });
  }

  @Override
  public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry geometry) {
    Objects.requireNonNull(geometry, "spatialObject");

    final Envelope envelope = geometry.getEnvelopeInternal();

    final List<KdTree> matchedPartitions = findLeafNodes(envelope);

    final Point point = geometry instanceof Point ? (Point) geometry : null;

    final Set<Tuple2<Integer, Geometry>> result = new HashSet<>();
    for (KdTree leaf : matchedPartitions) {
      // For points, make sure to return only one partition
      if (point != null && !(new HalfOpenRectangle(leaf.getBoundaryEnvelope())).contains(point)) {
        continue;
      }

      result.add(new Tuple2(leaf.getLeafId(), geometry));
    }

    return result.iterator();
  }

  @Override
  public List<Envelope> fetchLeafZones() {
    final List<Envelope> leafs = new ArrayList<>();
    this.traverse(
        tree -> {
          if (tree.isLeaf()) {
            leafs.add(tree.getBoundaryEnvelope());
          }
          return true;
        });
    return leafs;
  }

  /**
   * Traverses the tree top-down breadth-first and calls the visitor for each node. Stops traversing
   * if a call to Visitor.visit returns false.
   */
  public void traverse(Visitor<KdTree> visitor) {
    if (!visitor.visit(this)) {
      return;
    }

    if (left != null) {
      left.traverse(visitor);
    }
    if (right != null) {
      right.traverse(visitor);
    }
  }

  private boolean disjoint(Envelope r1, Envelope r2) {
    return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
  }

  private Envelope[] splitAt(boolean xAxis, double val) {
    return xAxis ? splitAtX(boundaryEnvelope, val) : splitAtY(boundaryEnvelope, val);
  }

  private double getKeyLowerBound(boolean xAxis) {
    return xAxis ? nodeKey.getMinX() : nodeKey.getMinY();
  }

  private double getExtendLowerBound(boolean xAxis) {
    return xAxis ? boundaryEnvelope.getMinX() : boundaryEnvelope.getMinY();
  }

  private double getExtendUpperBound(boolean xAxis) {
    return xAxis ? boundaryEnvelope.getMaxX() : boundaryEnvelope.getMaxY();
  }

  private Splitter getSplitter(boolean xAxis, double val) {
    return xAxis ? new XSplitter(val) : new YSplitter(val);
  }

  private interface Splitter {

    /**
     * @return true if the specified envelope belongs to the lower split
     */
    boolean split(Envelope envelope);
  }

  public static final class XSplitter implements Splitter {

    private final double x;

    private XSplitter(double x) {
      this.x = x;
    }

    @Override
    public boolean split(Envelope envelope) {
      return envelope.getMinX() <= x;
    }
  }

  public static final class YSplitter implements Splitter {

    private final double y;

    private YSplitter(double y) {
      this.y = y;
    }

    @Override
    public boolean split(Envelope envelope) {
      return envelope.getMinY() <= y;
    }
  }
}
