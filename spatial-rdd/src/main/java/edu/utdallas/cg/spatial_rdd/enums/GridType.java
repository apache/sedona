package edu.utdallas.cg.spatial_rdd.enums;

import org.apache.log4j.Logger;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/** The Enum GridType. */
public enum GridType implements Serializable {

  /** Partition the space to uniform grids */
  EQUALGRID,
  /** The Quad-Tree partitioning. */
  QUADTREE,

  /** K-D-B-tree partitioning (k-dimensional B-tree) */
  KDBTREE,

  KDTREE;


  /**
   * Gets the grid type.
   *
   * @param str the str
   * @return the grid type
   */
  public static GridType getGridType(String str) {
    final Logger logger = Logger.getLogger(GridType.class);
    for (GridType me : GridType.values()) {
      if (me.name().equalsIgnoreCase(str)) {
        return me;
      }
    }
    logger.error(
        "[Sedona] Choose quadtree or kdbtree instead. This grid type is not supported: " + str);
    return null;
  }
}
