package edu.utdallas.cg.spatial_rdd.core.tree.kdb;

import edu.utdallas.cg.spatial_rdd.core.tree.Tree;
import edu.utdallas.cg.spatial_rdd.core.tree.Visitor;
import edu.utdallas.cg.spatial_rdd.core.tree.kdb.support.PartitioningUtils;
import edu.utdallas.cg.spatial_rdd.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.*;

/** see https://en.wikipedia.org/wiki/K-D-B-tree */
public class KdBTree extends PartitioningUtils implements Tree<KdBTree> {

  private final int maxItemsPerNode;
  private final int maxLevels;
  private final Envelope extent;
  private final int level;
  private final List<Envelope> items = new ArrayList<>();
  private KdBTree[] children;
  private int leafId = 0;

  public KdBTree(int maxItemsPerNode, int maxLevels, Envelope extent) {
    this(maxItemsPerNode, maxLevels, 0, extent);
  }

  private KdBTree(int maxItemsPerNode, int maxLevels, int level, Envelope extent) {
    this.maxItemsPerNode = maxItemsPerNode;
    this.maxLevels = maxLevels;
    this.level = level;
    this.extent = extent;
  }

  @Override
  public boolean isLeaf() {
    return children == null;
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
    return extent;
  }

  @Override
  public void insert(Envelope envelope) {
    if (items.size() < maxItemsPerNode || level >= maxLevels) {
      items.add(envelope);
    } else {
      if (children == null) {
        // Split over longer side
        boolean splitX = extent.getWidth() > extent.getHeight();
        boolean ok = split(splitX);
        if (!ok) {
          // Try spitting by the other side
          ok = split(!splitX);
        }

        if (!ok) {
          // This could happen if all envelopes are the same.
          items.add(envelope);
          return;
        }
      }

      for (KdBTree child : children) {
        if (child.extent.contains(envelope.getMinX(), envelope.getMinY())) {
          child.insert(envelope);
          break;
        }
      }
    }
  }

  @Override
  public void dropElements() {
    traverse(
        new Visitor<KdBTree>() {
          @Override
          public boolean visit(KdBTree tree) {
            tree.items.clear();
            return true;
          }
        });
  }

  @Override
  public List<KdBTree> findLeafNodes(final Envelope envelope) {
    final List<KdBTree> matches = new ArrayList<>();
    traverse(
        new Visitor<KdBTree>() {
          @Override
          public boolean visit(KdBTree tree) {
            if (!disjoint(tree.getBoundaryEnvelope(), envelope)) {
              if (tree.isLeaf()) {
                matches.add(tree);
              }
              return true;
            } else {
              return false;
            }
          }
        });

    return matches;
  }

  private boolean disjoint(Envelope r1, Envelope r2) {
    return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
  }

  /**
   * Traverses the tree top-down breadth-first and calls the visitor for each node. Stops traversing
   * if a call to Visitor.visit returns false.
   */
  public void traverse(Visitor visitor) {
    if (!visitor.visit(this)) {
      return;
    }

    if (children != null) {
      for (KdBTree child : children) {
        child.traverse(visitor);
      }
    }
  }

  @Override
  public void assignLeafIds() {
    traverse(
        new Visitor<KdBTree>() {
          int id = 0;

          @Override
          public boolean visit(KdBTree tree) {
            if (tree.isLeaf()) {
              tree.leafId = id;
              id++;
            }
            return true;
          }
        });
  }

  private boolean split(boolean splitX) {
    final Comparator<Envelope> comparator = splitX ? new XComparator() : new YComparator();
    Collections.sort(items, comparator);

    final Envelope[] splits;
    final Splitter splitter;
    Envelope middleItem = items.get((int) Math.floor(items.size() / 2));
    if (splitX) {
      double x = middleItem.getMinX();
      if (x > extent.getMinX() && x < extent.getMaxX()) {
        splits = splitAtX(extent, x);
        splitter = new XSplitter(x);
      } else {
        // Too many objects are crowded at the edge of the extent. Can't split.
        return false;
      }
    } else {
      double y = middleItem.getMinY();
      if (y > extent.getMinY() && y < extent.getMaxY()) {
        splits = splitAtY(extent, y);
        splitter = new YSplitter(y);
      } else {
        // Too many objects are crowded at the edge of the extent. Can't split.
        return false;
      }
    }

    children = new KdBTree[2];
    children[0] = new KdBTree(maxItemsPerNode, maxLevels, level + 1, splits[0]);
    children[1] = new KdBTree(maxItemsPerNode, maxLevels, level + 1, splits[1]);

    // Move items
    splitItems(splitter);
    return true;
  }

  private void splitItems(Splitter splitter) {
    for (Envelope item : items) {
      children[splitter.split(item) ? 0 : 1].insert(item);
    }
  }

  private Envelope[] splitAtX(Envelope envelope, double x) {
    assert (envelope.getMinX() < x);
    assert (envelope.getMaxX() > x);
    Envelope[] splits = new Envelope[2];
    splits[0] = new Envelope(envelope.getMinX(), x, envelope.getMinY(), envelope.getMaxY());
    splits[1] = new Envelope(x, envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
    return splits;
  }

  private Envelope[] splitAtY(Envelope envelope, double y) {
    assert (envelope.getMinY() < y);
    assert (envelope.getMaxY() > y);
    Envelope[] splits = new Envelope[2];
    splits[0] = new Envelope(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), y);
    splits[1] = new Envelope(envelope.getMinX(), envelope.getMaxX(), y, envelope.getMaxY());
    return splits;
  }

  @Override
  public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry geometry) {
    Objects.requireNonNull(geometry, "spatialObject");

    final Envelope envelope = geometry.getEnvelopeInternal();

    final List<KdBTree> matchedPartitions = findLeafNodes(envelope);

    final Point point = geometry instanceof Point ? (Point) geometry : null;

    final Set<Tuple2<Integer, Geometry>> result = new HashSet<>();
    for (KdBTree leaf : matchedPartitions) {
      // For points, make sure to return only one partition
      if (point != null && !(new HalfOpenRectangle(leaf.getBoundaryEnvelope())).contains(point)) {
        continue;
      }

      result.add(new Tuple2(leaf.getLeafId(), geometry));
    }

    return result.iterator();
  }

  @Override
  public Set<Integer> getKeys(Geometry geometry) {
    Objects.requireNonNull(geometry, "spatialObject");

    final Envelope envelope = geometry.getEnvelopeInternal();

    final List<KdBTree> matchedPartitions = findLeafNodes(envelope);

    final Point point = geometry instanceof Point ? (Point) geometry : null;

    final Set<Integer> result = new HashSet<>();
    for (KdBTree leaf : matchedPartitions) {
      // For points, make sure to return only one partition
      if (point != null && !(new HalfOpenRectangle(leaf.getBoundaryEnvelope())).contains(point)) {
        continue;
      }

      result.add(leaf.getLeafId());
    }
    return result;
  }

  @Override
  public List<Envelope> fetchLeafZones() {
    final List<Envelope> leafs = new ArrayList<>();
    this.traverse(
        new Visitor<KdBTree>() {
          @Override
          public boolean visit(KdBTree tree) {
            if (tree.isLeaf()) {
              leafs.add(tree.getBoundaryEnvelope());
            }
            return true;
          }
        });
    return leafs;
  }

  private interface Splitter {
    /**
     * @return true if the specified envelope belongs to the lower split
     */
    boolean split(Envelope envelope);
  }

  public static final class XComparator implements Comparator<Envelope> {
    @Override
    public int compare(Envelope o1, Envelope o2) {
      double deltaX = o1.getMinX() - o2.getMinX();
      return (int) Math.signum(deltaX != 0 ? deltaX : o1.getMinY() - o2.getMinY());
    }
  }

  public static final class YComparator implements Comparator<Envelope> {
    @Override
    public int compare(Envelope o1, Envelope o2) {
      double deltaY = o1.getMinY() - o2.getMinY();
      return (int) Math.signum(deltaY != 0 ? deltaY : o1.getMinX() - o2.getMinX());
    }
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
