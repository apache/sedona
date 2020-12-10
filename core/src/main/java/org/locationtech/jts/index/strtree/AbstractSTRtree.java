
/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.locationtech.jts.index.strtree;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.util.Assert;

/**
 * Base class for STRtree and SIRtree. STR-packed R-trees are described in:
 * P. Rigaux, Michel Scholl and Agnes Voisard. <i>Spatial Databases With
 * Application To GIS.</i> Morgan Kaufmann, San Francisco, 2002.
 * <p>
 * This implementation is based on {@link Boundable}s rather than {@link AbstractNode}s,
 * because the STR algorithm operates on both nodes and
 * data, both of which are treated as Boundables.
 * <p>
 * This class is thread-safe.  Building the tree is synchronized, 
 * and querying is stateless.
 *
 * @see STRtree
 * @see SIRtree
 *
 * @version 1.7
 */
public abstract class AbstractSTRtree implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -3886435814360241337L;

  /**
   * A test for intersection between two bounds, necessary because subclasses
   * of AbstractSTRtree have different implementations of bounds.
   */
  protected static interface IntersectsOp {
    /**
     * For STRtrees, the bounds will be Envelopes; for SIRtrees, Intervals;
     * for other subclasses of AbstractSTRtree, some other class.
     * @param aBounds the bounds of one spatial object
     * @param bBounds the bounds of another spatial object
     * @return whether the two bounds intersect
     */
    boolean intersects(Object aBounds, Object bBounds);
  }

  protected AbstractNode root;

  private boolean built = false;
  /**
   * Set to <tt>null</tt> when index is built, to avoid retaining memory.
   */
  private ArrayList itemBoundables = new ArrayList();
  
  private int nodeCapacity;

  private static final int DEFAULT_NODE_CAPACITY = 10;

  /**
   * Constructs an AbstractSTRtree with the 
   * default node capacity.
   */
  public AbstractSTRtree() {
    this(DEFAULT_NODE_CAPACITY);
  }

  /**
   * Constructs an AbstractSTRtree with the specified maximum number of child
   * nodes that a node may have
   * 
   * @param nodeCapacity the maximum number of child nodes in a node
   */
  public AbstractSTRtree(int nodeCapacity) {
    Assert.isTrue(nodeCapacity > 1, "Node capacity must be greater than 1");
    this.nodeCapacity = nodeCapacity;
  }

  /**
   * Constructs an AbstractSTRtree with the specified maximum number of child
   * nodes that a node may have, and the root node
   * @param nodeCapacity the maximum number of child nodes in a node
   * @param root the root node that links to all other nodes in the tree
   */
  public AbstractSTRtree(int nodeCapacity, AbstractNode root) {
    this(nodeCapacity);
    built = true;
    this.root = root;
    this.itemBoundables = null;
  }

  /**
   * Constructs an AbstractSTRtree with the specified maximum number of child
   * nodes that a node may have, and all leaf nodes in the tree
   * @param nodeCapacity the maximum number of child nodes in a node
   * @param itemBoundables the list of leaf nodes in the tree
   */
  public AbstractSTRtree(int nodeCapacity, ArrayList itemBoundables) {
    this(nodeCapacity);
    this.itemBoundables = itemBoundables;
  }
  /**
   * Creates parent nodes, grandparent nodes, and so forth up to the root
   * node, for the data that has been inserted into the tree. Can only be
   * called once, and thus can be called only after all of the data has been
   * inserted into the tree.
   */
  public synchronized void build() {
    if (built) return;
    root = itemBoundables.isEmpty()
           ? createNode(0)
           : createHigherLevels(itemBoundables, -1);
    // the item list is no longer needed
    itemBoundables = null;
    built = true;
  }

  protected abstract AbstractNode createNode(int level);

  /**
   * Sorts the childBoundables then divides them into groups of size M, where
   * M is the node capacity.
   */
  protected List createParentBoundables(List childBoundables, int newLevel) {
    Assert.isTrue(!childBoundables.isEmpty());
    ArrayList parentBoundables = new ArrayList();
    parentBoundables.add(createNode(newLevel));
    ArrayList sortedChildBoundables = new ArrayList(childBoundables);
    Collections.sort(sortedChildBoundables, getComparator());
    for (Iterator i = sortedChildBoundables.iterator(); i.hasNext(); ) {
      Boundable childBoundable = (Boundable) i.next();
      if (lastNode(parentBoundables).getChildBoundables().size() == getNodeCapacity()) {
        parentBoundables.add(createNode(newLevel));
      }
      lastNode(parentBoundables).addChildBoundable(childBoundable);
    }
    return parentBoundables;
  }

  protected AbstractNode lastNode(List nodes) {
    return (AbstractNode) nodes.get(nodes.size() - 1);
  }

  protected static int compareDoubles(double a, double b) {
    return a > b ? 1
         : a < b ? -1
         : 0;
  }

  /**
   * Creates the levels higher than the given level
   *
   * @param boundablesOfALevel
   *            the level to build on
   * @param level
   *            the level of the Boundables, or -1 if the boundables are item
   *            boundables (that is, below level 0)
   * @return the root, which may be a ParentNode or a LeafNode
   */
  private AbstractNode createHigherLevels(List boundablesOfALevel, int level) {
    Assert.isTrue(!boundablesOfALevel.isEmpty());
    List parentBoundables = createParentBoundables(boundablesOfALevel, level + 1);
    if (parentBoundables.size() == 1) {
      return (AbstractNode) parentBoundables.get(0);
    }
    return createHigherLevels(parentBoundables, level + 1);
  }

  /**
   * Gets the root node of the tree.
   * 
   * @return the root node
   */
  public AbstractNode getRoot() 
  {
    build();
    return root; 
  }

  /**
   * Returns the maximum number of child nodes that a node may have.
   * 
   * @return the node capacity
   */
  public int getNodeCapacity() { return nodeCapacity; }

  /**
   * Tests whether the index contains any items.
   * This method does not build the index,
   * so items can still be inserted after it has been called.
   * 
   * @return true if the index does not contain any items
   */
  public boolean isEmpty()
  {
    if (! built) return itemBoundables.isEmpty();
    return root.isEmpty();
  }
  
  protected int size() {
    if (isEmpty()) {
      return 0;
    }
    build();
    return size(root);
  }

  protected int size(AbstractNode node)
  {
    int size = 0;
    for (Iterator i = node.getChildBoundables().iterator(); i.hasNext(); ) {
      Boundable childBoundable = (Boundable) i.next();
      if (childBoundable instanceof AbstractNode) {
        size += size((AbstractNode) childBoundable);
      }
      else if (childBoundable instanceof ItemBoundable) {
        size += 1;
      }
    }
    return size;
  }

  protected int depth() {
    if (isEmpty()) {
      return 0;
    }
    build();
    return depth(root);
  }

  protected int depth(AbstractNode node)
  {
    int maxChildDepth = 0;
    for (Iterator i = node.getChildBoundables().iterator(); i.hasNext(); ) {
      Boundable childBoundable = (Boundable) i.next();
      if (childBoundable instanceof AbstractNode) {
        int childDepth = depth((AbstractNode) childBoundable);
        if (childDepth > maxChildDepth)
          maxChildDepth = childDepth;
      }
    }
    return maxChildDepth + 1;
  }


  protected void insert(Object bounds, Object item) {
    Assert.isTrue(!built, "Cannot insert items into an STR packed R-tree after it has been built.");
    itemBoundables.add(new ItemBoundable(bounds, item));
  }

  /**
   *  Also builds the tree, if necessary.
   */
  protected List query(Object searchBounds) {
    build();
    ArrayList matches = new ArrayList();
    if (isEmpty()) {
      //Assert.isTrue(root.getBounds() == null);
      return matches;
    }
    if (getIntersectsOp().intersects(root.getBounds(), searchBounds)) {
      queryInternal(searchBounds, root, matches);
    }
    return matches;
  }

  /**
   *  Also builds the tree, if necessary.
   */
  protected void query(Object searchBounds, ItemVisitor visitor) {
    build();
    if (isEmpty()) {
      // nothing in tree, so return
      //Assert.isTrue(root.getBounds() == null);
      return;
    }
    if (getIntersectsOp().intersects(root.getBounds(), searchBounds)) {
      queryInternal(searchBounds, root, visitor);
    }
  }

  /**
   * @return a test for intersection between two bounds, necessary because subclasses
   * of AbstractSTRtree have different implementations of bounds.
   * @see IntersectsOp
   */
  protected abstract IntersectsOp getIntersectsOp();

  private void queryInternal(Object searchBounds, AbstractNode node, List matches) {
    List childBoundables = node.getChildBoundables();
    for (int i = 0; i < childBoundables.size(); i++) {
      Boundable childBoundable = (Boundable) childBoundables.get(i);
      if (! getIntersectsOp().intersects(childBoundable.getBounds(), searchBounds)) {
        continue;
      }
      if (childBoundable instanceof AbstractNode) {
        queryInternal(searchBounds, (AbstractNode) childBoundable, matches);
      }
      else if (childBoundable instanceof ItemBoundable) {
        matches.add(((ItemBoundable)childBoundable).getItem());
      }
      else {
        Assert.shouldNeverReachHere();
      }
    }
  }

  private void queryInternal(Object searchBounds, AbstractNode node, ItemVisitor visitor) {
    List childBoundables = node.getChildBoundables();
    for (int i = 0; i < childBoundables.size(); i++) {
      Boundable childBoundable = (Boundable) childBoundables.get(i);
      if (! getIntersectsOp().intersects(childBoundable.getBounds(), searchBounds)) {
        continue;
      }
      if (childBoundable instanceof AbstractNode) {
        queryInternal(searchBounds, (AbstractNode) childBoundable, visitor);
      }
      else if (childBoundable instanceof ItemBoundable) {
        visitor.visitItem(((ItemBoundable)childBoundable).getItem());
      }
      else {
        Assert.shouldNeverReachHere();
      }
    }
  }

  /**
   * Gets a tree structure (as a nested list) 
   * corresponding to the structure of the items and nodes in this tree.
   * <p>
   * The returned {@link List}s contain either {@link Object} items, 
   * or Lists which correspond to subtrees of the tree
   * Subtrees which do not contain any items are not included.
   * <p>
   * Builds the tree if necessary.
   * 
   * @return a List of items and/or Lists
   */
  public List itemsTree()
  {
    build();

    List valuesTree = itemsTree(root);
    if (valuesTree == null)
      return new ArrayList();
    return valuesTree;
  }
  
  private List itemsTree(AbstractNode node) 
  {
    List valuesTreeForNode = new ArrayList();
    for (Iterator i = node.getChildBoundables().iterator(); i.hasNext(); ) {
      Boundable childBoundable = (Boundable) i.next();
      if (childBoundable instanceof AbstractNode) {
        List valuesTreeForChild = itemsTree((AbstractNode) childBoundable);
        // only add if not null (which indicates an item somewhere in this tree
        if (valuesTreeForChild != null)
          valuesTreeForNode.add(valuesTreeForChild);
      }
      else if (childBoundable instanceof ItemBoundable) {
        valuesTreeForNode.add(((ItemBoundable)childBoundable).getItem());
      }
      else {
        Assert.shouldNeverReachHere();
      }
    }
    if (valuesTreeForNode.size() <= 0) 
      return null;
    return valuesTreeForNode;
  }

  /**
   * Removes an item from the tree.
   * (Builds the tree, if necessary.)
   */
  protected boolean remove(Object searchBounds, Object item) {
    build();
    if (getIntersectsOp().intersects(root.getBounds(), searchBounds)) {
      return remove(searchBounds, root, item);
    }
    return false;
  }

  private boolean removeItem(AbstractNode node, Object item)
  {
    Boundable childToRemove = null;
    for (Iterator i = node.getChildBoundables().iterator(); i.hasNext(); ) {
      Boundable childBoundable = (Boundable) i.next();
      if (childBoundable instanceof ItemBoundable) {
        if ( ((ItemBoundable) childBoundable).getItem() == item)
          childToRemove = childBoundable;
      }
    }
    if (childToRemove != null) {
      node.getChildBoundables().remove(childToRemove);
      return true;
    }
    return false;
  }

  private boolean remove(Object searchBounds, AbstractNode node, Object item) {
    // first try removing item from this node
    boolean found = removeItem(node, item);
    if (found)
      return true;

    AbstractNode childToPrune = null;
    // next try removing item from lower nodes
    for (Iterator i = node.getChildBoundables().iterator(); i.hasNext(); ) {
      Boundable childBoundable = (Boundable) i.next();
      if (!getIntersectsOp().intersects(childBoundable.getBounds(), searchBounds)) {
        continue;
      }
      if (childBoundable instanceof AbstractNode) {
        found = remove(searchBounds, (AbstractNode) childBoundable, item);
        // if found, record child for pruning and exit
        if (found) {
          childToPrune = (AbstractNode) childBoundable;
          break;
        }
      }
    }
    // prune child if possible
    if (childToPrune != null) {
      if (childToPrune.getChildBoundables().isEmpty()) {
        node.getChildBoundables().remove(childToPrune);
      }
    }
    return found;
  }

  protected List boundablesAtLevel(int level) {
    ArrayList boundables = new ArrayList();
    boundablesAtLevel(level, root, boundables);
    return boundables;
  }

  /**
   * @param level -1 to get items
   */
  private void boundablesAtLevel(int level, AbstractNode top, Collection boundables) {
    Assert.isTrue(level > -2);
    if (top.getLevel() == level) {
      boundables.add(top);
      return;
    }
    for (Iterator i = top.getChildBoundables().iterator(); i.hasNext(); ) {
      Boundable boundable = (Boundable) i.next();
      if (boundable instanceof AbstractNode) {
        boundablesAtLevel(level, (AbstractNode)boundable, boundables);
      }
      else {
        Assert.isTrue(boundable instanceof ItemBoundable);
        if (level == -1) { boundables.add(boundable); }
      }
    }
    return;
  }

  protected abstract Comparator getComparator();

  ArrayList getItemBoundables()
  {
    return itemBoundables;
  }
}
