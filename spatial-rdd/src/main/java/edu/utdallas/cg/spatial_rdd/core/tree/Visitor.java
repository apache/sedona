package edu.utdallas.cg.spatial_rdd.core.tree;

public interface Visitor<T> {
    /**
     * Visits a single node of the tree
     *
     * @param tree Node to visit
     * @return true to continue traversing the tree; false to stop
     */
    boolean visit(T tree);
  }