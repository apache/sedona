
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.util.Assert;


/**
 *  A query-only R-tree created using the Sort-Tile-Recursive (STR) algorithm.
 *  For two-dimensional spatial data.
 * <P>
 *  The STR packed R-tree is simple to implement and maximizes space
 *  utilization; that is, as many leaves as possible are filled to capacity.
 *  Overlap between nodes is far less than in a basic R-tree. However, once the
 *  tree has been built (explicitly or on the first call to #query), items may
 *  not be added or removed.
 * <P>
 * Described in: P. Rigaux, Michel Scholl and Agnes Voisard.
 * <i>Spatial Databases With Application To GIS</i>.
 * Morgan Kaufmann, San Francisco, 2002.
 * <p>
 * <b>Note that inserting items into a tree is not thread-safe.</b>
 * Inserting performed on more than one thread must be synchronized externally.
 * <p>
 * Querying a tree is thread-safe.  
 * The building phase is done synchronously, 
 * and querying is stateless.
 *
 * @version 1.7
 */
public class STRtree extends AbstractSTRtree 
implements SpatialIndex, Serializable 
{

  static final class STRtreeNode extends AbstractNode
  {
    STRtreeNode(int level)
    {
      super(level);
    }

    protected Object computeBounds() {
      Envelope bounds = null;
      for (Iterator i = getChildBoundables().iterator(); i.hasNext(); ) {
        Boundable childBoundable = (Boundable) i.next();
        if (bounds == null) {
          bounds = new Envelope((Envelope)childBoundable.getBounds());
        }
        else {
          bounds.expandToInclude((Envelope)childBoundable.getBounds());
        }
      }
      return bounds;
    }
  }

  /**
   * 
   */
  private static final long serialVersionUID = 259274702368956900L;
  
  private static Comparator xComparator =
    new Comparator() {
      public int compare(Object o1, Object o2) {
        return compareDoubles(
            centreX((Envelope)((Boundable)o1).getBounds()),
            centreX((Envelope)((Boundable)o2).getBounds()));
      }
    };
  private static Comparator yComparator =
    new Comparator() {
      public int compare(Object o1, Object o2) {
        return compareDoubles(
            centreY((Envelope)((Boundable)o1).getBounds()),
            centreY((Envelope)((Boundable)o2).getBounds()));
      }
    };

  private static double centreX(Envelope e) {
    return avg(e.getMinX(), e.getMaxX());
  }

  private static double centreY(Envelope e) {
    return avg(e.getMinY(), e.getMaxY());
  }

  private static double avg(double a, double b) { return (a + b) / 2d; }

  private static IntersectsOp intersectsOp = new IntersectsOp() {
    public boolean intersects(Object aBounds, Object bBounds) {
      return ((Envelope)aBounds).intersects((Envelope)bBounds);
    }
  };

  /**
   * Creates the parent level for the given child level. First, orders the items
   * by the x-values of the midpoints, and groups them into vertical slices.
   * For each slice, orders the items by the y-values of the midpoints, and
   * group them into runs of size M (the node capacity). For each run, creates
   * a new (parent) node.
   */
  protected List createParentBoundables(List childBoundables, int newLevel) {
    Assert.isTrue(!childBoundables.isEmpty());
    int minLeafCount = (int) Math.ceil((childBoundables.size() / (double) getNodeCapacity()));
    ArrayList sortedChildBoundables = new ArrayList(childBoundables);
    Collections.sort(sortedChildBoundables, xComparator);
    List[] verticalSlices = verticalSlices(sortedChildBoundables,
        (int) Math.ceil(Math.sqrt(minLeafCount)));
    return createParentBoundablesFromVerticalSlices(verticalSlices, newLevel);
  }

  private List createParentBoundablesFromVerticalSlices(List[] verticalSlices, int newLevel) {
    Assert.isTrue(verticalSlices.length > 0);
    List parentBoundables = new ArrayList();
    for (int i = 0; i < verticalSlices.length; i++) {
      parentBoundables.addAll(
            createParentBoundablesFromVerticalSlice(verticalSlices[i], newLevel));
    }
    return parentBoundables;
  }

  protected List createParentBoundablesFromVerticalSlice(List childBoundables, int newLevel) {
    return super.createParentBoundables(childBoundables, newLevel);
  }

  /**
   * @param childBoundables Must be sorted by the x-value of the envelope midpoints
   */
  protected List[] verticalSlices(List childBoundables, int sliceCount) {
    int sliceCapacity = (int) Math.ceil(childBoundables.size() / (double) sliceCount);
    List[] slices = new List[sliceCount];
    Iterator i = childBoundables.iterator();
    for (int j = 0; j < sliceCount; j++) {
      slices[j] = new ArrayList();
      int boundablesAddedToSlice = 0;
      while (i.hasNext() && boundablesAddedToSlice < sliceCapacity) {
        Boundable childBoundable = (Boundable) i.next();
        slices[j].add(childBoundable);
        boundablesAddedToSlice++;
      }
    }
    return slices;
  }

  private static final int DEFAULT_NODE_CAPACITY = 10;
  
  /**
   * Constructs an STRtree with the default node capacity.
   */
  public STRtree() 
  { 
    this(DEFAULT_NODE_CAPACITY); 
  }

  /**
   * Constructs an STRtree with the given maximum number of child nodes that
   * a node may have.
   * <p>
   * The minimum recommended capacity setting is 4.
   * 
   */
  public STRtree(int nodeCapacity) {
    super(nodeCapacity);
  }

  /**
   * Constructs an STRtree with the given maximum number of child nodes that
   * a node may have, and the root that links to all other nodes
   * <p>
   * The minimum recommended capacity setting is 4.
   *
   */
  public STRtree(int nodeCapacity, STRtreeNode root) {
    super(nodeCapacity, root);
  }

  /**
   * Constructs an STRtree with the given maximum number of child nodes that
   * a node may have, and all leaf nodes in the tree
   * <p>
   * The minimum recommended capacity setting is 4.
   *
   */
  public STRtree(int nodeCapacity, ArrayList itemBoundables) {
    super(nodeCapacity, itemBoundables);
  }

  protected AbstractNode createNode(int level) {
    return new STRtreeNode(level);
  }

  protected IntersectsOp getIntersectsOp() {
    return intersectsOp;
  }

  /**
   * Inserts an item having the given bounds into the tree.
   */
  public void insert(Envelope itemEnv, Object item) {
    if (itemEnv.isNull()) { return; }
    super.insert(itemEnv, item);
  }

  /**
   * Returns items whose bounds intersect the given envelope.
   */
  public List query(Envelope searchEnv) {
    //Yes this method does something. It specifies that the bounds is an
    //Envelope. super.query takes an Object, not an Envelope. [Jon Aquino 10/24/2003]
    return super.query(searchEnv);
  }

  /**
   * Returns items whose bounds intersect the given envelope.
   */
  public void query(Envelope searchEnv, ItemVisitor visitor) {
    //Yes this method does something. It specifies that the bounds is an
    //Envelope. super.query takes an Object, not an Envelope. [Jon Aquino 10/24/2003]
    super.query(searchEnv, visitor);
  }

  /**
   * Removes a single item from the tree.
   *
   * @param itemEnv the Envelope of the item to remove
   * @param item the item to remove
   * @return <code>true</code> if the item was found
   */
  public boolean remove(Envelope itemEnv, Object item) {
    return super.remove(itemEnv, item);
  }

  /**
   * Returns the number of items in the tree.
   *
   * @return the number of items in the tree
   */
  public int size()
  {
    return super.size();
  }

  /**
   * Returns the number of items in the tree.
   *
   * @return the number of items in the tree
   */
  public int depth()
  {
    return super.depth();
  }

  protected Comparator getComparator() {
    return yComparator;
  }

  /**
   * Finds the two nearest items in the tree, 
   * using {@link ItemDistance} as the distance metric.
   * A Branch-and-Bound tree traversal algorithm is used
   * to provide an efficient search.
   * <p>
   * If the tree is empty, the return value is <code>null</code.
   * If the tree contains only one item, 
   * the return value is a pair containing that item.  
   * <b>
   * If it is required to find only pairs of distinct items,
   * the {@link ItemDistance} function must be <b>anti-reflexive</b>.
   * 
   * @param itemDist a distance metric applicable to the items in this tree
   * @return the pair of the nearest items
   *    or <code>null</code> if the tree is empty
   */
  public Object[] nearestNeighbour(ItemDistance itemDist)
  {
    if (isEmpty()) return null;
    
    // if tree has only one item this will return null
    BoundablePair bp = new BoundablePair(this.getRoot(), this.getRoot(), itemDist);
    return nearestNeighbour(bp);
  }

  /**
   * Finds the item in this tree which is nearest to the given {@link Object}, 
   * using {@link ItemDistance} as the distance metric.
   * A Branch-and-Bound tree traversal algorithm is used
   * to provide an efficient search.
   * <p>
   * The query <tt>object</tt> does <b>not</b> have to be 
   * contained in the tree, but it does 
   * have to be compatible with the <tt>itemDist</tt> 
   * distance metric. 
   * 
   * @param env the envelope of the query item
   * @param item the item to find the nearest neighbour of
   * @param itemDist a distance metric applicable to the items in this tree and the query item
   * @return the nearest item in this tree
   *    or <code>null</code> if the tree is empty
   */
  public Object nearestNeighbour(Envelope env, Object item, ItemDistance itemDist)
  {
    Boundable bnd = new ItemBoundable(env, item);
    BoundablePair bp = new BoundablePair(this.getRoot(), bnd, itemDist);
    return nearestNeighbour(bp)[0];
  }
  
  /**
   * Finds the two nearest items from this tree 
   * and another tree,
   * using {@link ItemDistance} as the distance metric.
   * A Branch-and-Bound tree traversal algorithm is used
   * to provide an efficient search.
   * The result value is a pair of items, 
   * the first from this tree and the second
   * from the argument tree.
   * 
   * @param tree another tree
   * @param itemDist a distance metric applicable to the items in the trees
   * @return the pair of the nearest items, one from each tree
   *    or <code>null</code> if no pair of distinct items can be found
   */
  public Object[] nearestNeighbour(STRtree tree, ItemDistance itemDist)
  {
    if (isEmpty() || tree.isEmpty()) return null;
    BoundablePair bp = new BoundablePair(this.getRoot(), tree.getRoot(), itemDist);
    return nearestNeighbour(bp);
  }
  
  private Object[] nearestNeighbour(BoundablePair initBndPair) 
  {
    double distanceLowerBound = Double.POSITIVE_INFINITY;
    BoundablePair minPair = null;
    
    // initialize search queue
    PriorityQueue priQ = new PriorityQueue();
    priQ.add(initBndPair);

    while (! priQ.isEmpty() && distanceLowerBound > 0.0) {
      // pop head of queue and expand one side of pair
      BoundablePair bndPair = (BoundablePair) priQ.poll();
      double pairDistance = bndPair.getDistance();
      
      /**
       * If the distance for the first pair in the queue
       * is >= current minimum distance, other nodes
       * in the queue must also have a greater distance.
       * So the current minDistance must be the true minimum,
       * and we are done.
       */
      if (pairDistance >= distanceLowerBound) 
        break;  

      /**
       * If the pair members are leaves
       * then their distance is the exact lower bound.
       * Update the distanceLowerBound to reflect this
       * (which must be smaller, due to the test 
       * immediately prior to this). 
       */
      if (bndPair.isLeaves()) {
        // assert: currentDistance < minimumDistanceFound
        distanceLowerBound = pairDistance;
        minPair = bndPair;
      }
      else {
        /**
         * Otherwise, expand one side of the pair, 
         * and insert the expanded pairs into the queue.
         * The choice of which side to expand is determined heuristically.
         */
        bndPair.expandToQueue(priQ, distanceLowerBound);
      }
    }
    if (minPair == null) 
      return null;
    // done - return items with min distance
    return new Object[] {    
          ((ItemBoundable) minPair.getBoundable(0)).getItem(),
          ((ItemBoundable) minPair.getBoundable(1)).getItem()
      };
  }
  
  /**
   * Tests whether some two items from this tree and another tree
   * lie within a given distance.
   * {@link ItemDistance} is used as the distance metric.
   * A Branch-and-Bound tree traversal algorithm is used
   * to provide an efficient search.
   * 
   * @param tree another tree
   * @param itemDist a distance metric applicable to the items in the trees
   * @param maxDistance the distance limit for the search
   * @return true if there are items within the distance
   */
  public boolean isWithinDistance(STRtree tree, ItemDistance itemDist, double maxDistance)
  {
    BoundablePair bp = new BoundablePair(this.getRoot(), tree.getRoot(), itemDist);
    return isWithinDistance(bp, maxDistance);
  }
  
  /**
   * Performs a withinDistance search on the tree node pairs.
   * This is a different search algorithm to nearest neighbour.
   * It can utilize the {@link BoundablePair#maximumDistance()} between
   * tree nodes to confirm if two internal nodes must
   * have items closer than the maxDistance,
   * and short-circuit the search.
   * 
   * @param initBndPair the initial pair containing the tree root nodes
   * @param maxDistance the maximum distance to search for
   * @return true if two items lie within the given distance
   */
  private boolean isWithinDistance(BoundablePair initBndPair, double maxDistance) 
  {
    double distanceUpperBound = Double.POSITIVE_INFINITY;
    
    // initialize search queue
    PriorityQueue priQ = new PriorityQueue();
    priQ.add(initBndPair);

    while (! priQ.isEmpty()) {
      // pop head of queue and expand one side of pair
      BoundablePair bndPair = (BoundablePair) priQ.poll();
      double pairDistance = bndPair.getDistance();
      
      /**
       * If the distance for the first pair in the queue
       * is > maxDistance, all other pairs
       * in the queue must have a greater distance as well.
       * So can conclude no items are within the distance
       * and terminate with result = false
       */
      if (pairDistance > maxDistance) 
        return false;  

      /**
       * If the maximum distance between the nodes
       * is less than the maxDistance,
       * than all items in the nodes must be 
       * closer than the max distance.
       * Then can terminate with result = true.
       * 
       * NOTE: using Envelope MinMaxDistance 
       * would provide a tighter bound,
       * but not much performance improvement has been observed
       */
      if (bndPair.maximumDistance() <= maxDistance)
        return true;
      /**
       * If the pair items are leaves
       * then their actual distance is an upper bound.
       * Update the distanceUpperBound to reflect this
       */
      if (bndPair.isLeaves()) {
        // assert: currentDistance < minimumDistanceFound
        distanceUpperBound = pairDistance;
        
        /**
         * If the items are closer than maxDistance
         * can terminate with result = true.
         */
        if (distanceUpperBound <= maxDistance)
          return true;
      }
      else {
        /**
         * Otherwise, expand one side of the pair, 
         * and insert the expanded pairs into the queue.
         * The choice of which side to expand is determined heuristically.
         */
        bndPair.expandToQueue(priQ, distanceUpperBound);
      }
    }
    return false;
  }
 
  /**
   * Finds k items in this tree which are the top k nearest neighbors to the given {@code item}, 
   * using {@code itemDist} as the distance metric.
   * A Branch-and-Bound tree traversal algorithm is used
   * to provide an efficient search.
   * This method implements the KNN algorithm described in the following paper:
   * <p>
   * Roussopoulos, Nick, Stephen Kelley, and Frédéric Vincent. "Nearest neighbor queries."
   * ACM sigmod record. Vol. 24. No. 2. ACM, 1995.
   * <p>
   * The query {@code item} does <b>not</b> have to be 
   * contained in the tree, but it does 
   * have to be compatible with the {@code itemDist} 
   * distance metric. 
   * 
   * @param env the envelope of the query item
   * @param item the item to find the nearest neighbour of
   * @param itemDist a distance metric applicable to the items in this tree and the query item
   * @param k the K nearest items in kNearestNeighbour
   * @return the K nearest items in this tree
   */
  public Object[] nearestNeighbour(Envelope env, Object item, ItemDistance itemDist,int k)
  {
    Boundable bnd = new ItemBoundable(env, item);
    BoundablePair bp = new BoundablePair(this.getRoot(), bnd, itemDist);
    return nearestNeighbourK(bp,k);
  }

  private Object[] nearestNeighbourK(BoundablePair initBndPair, int k) 
  {
    return nearestNeighbourK(initBndPair, Double.POSITIVE_INFINITY,k);
  }
  
  private Object[] nearestNeighbourK(BoundablePair initBndPair, double maxDistance, int k) 
  {
    double distanceLowerBound = maxDistance;
    
    // initialize internal structures
    PriorityQueue priQ = new PriorityQueue();

    // initialize queue
    priQ.add(initBndPair);

    PriorityQueue kNearestNeighbors = new PriorityQueue();

    while (! priQ.isEmpty() && distanceLowerBound >= 0.0) {
      // pop head of queue and expand one side of pair
      BoundablePair bndPair = (BoundablePair) priQ.poll();
      double pairDistance = bndPair.getDistance();
      
      
      /**
       * If the distance for the first node in the queue
       * is >= the current maximum distance in the k queue , all other nodes
       * in the queue must also have a greater distance.
       * So the current minDistance must be the true minimum,
       * and we are done.
       */
      if (pairDistance >= distanceLowerBound){
    	  break;  
      }
      /**
       * If the pair members are leaves
       * then their distance is the exact lower bound.
       * Update the distanceLowerBound to reflect this
       * (which must be smaller, due to the test 
       * immediately prior to this). 
       */
      if (bndPair.isLeaves()) {
        // assert: currentDistance < minimumDistanceFound
    	
    	  if(kNearestNeighbors.size()<k){
	    	  	kNearestNeighbors.add(bndPair);
    	  }
    	  else
    	  {

          BoundablePair bp1 = (BoundablePair) kNearestNeighbors.peek();
          if(bp1.getDistance() > pairDistance) {
    			  kNearestNeighbors.poll();
    			  kNearestNeighbors.add(bndPair);
    		  }
    		  /*
    		   * minDistance should be the farthest point in the K nearest neighbor queue.
    		   */
          BoundablePair bp2 = (BoundablePair) kNearestNeighbors.peek();
    		  distanceLowerBound = bp2.getDistance();
    	  }        
      }
      else {
        /**
         * Otherwise, expand one side of the pair,
         * (the choice of which side to expand is heuristically determined) 
         * and insert the new expanded pairs into the queue
         */
        bndPair.expandToQueue(priQ, distanceLowerBound);
      }
    }
    // done - return items with min distance

    return getItems(kNearestNeighbors);
  }
  private static Object[] getItems(PriorityQueue kNearestNeighbors)
  {
	  /** 
	   * Iterate the K Nearest Neighbour Queue and retrieve the item from each BoundablePair
	   * in this queue
	   */
	  Object[] items = new Object[kNearestNeighbors.size()];
	  int count=0;
	  while( ! kNearestNeighbors.isEmpty() )
	  {
      BoundablePair bp = (BoundablePair) kNearestNeighbors.poll(); 
      items[count]=((ItemBoundable)bp.getBoundable(0)).getItem();
      count++;
	  }	
	  return items;
  }
}
 
