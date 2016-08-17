
/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package com.vividsolutions.jts.index.strtree;

import com.vividsolutions.jts.index.strtree.AbstractSTRtree;

import java.io.Serializable;
import java.util.*;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.*;
import com.vividsolutions.jts.util.PriorityQueue;
import com.vividsolutions.jts.index.*;

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
 * This class is thread-safe.  Building the tree is synchronized, 
 * and querying is stateless.
 *
 * @version 1.7
 */
public class STRtree extends AbstractSTRtree 
implements SpatialIndex, Serializable 
{

  private static final class STRtreeNode extends AbstractNode
  {
    private STRtreeNode(int level)
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
   * 
   * @param itemDist a distance metric applicable to the items in this tree
   * @return the pair of the nearest items
   */
  public Object[] nearestNeighbour(ItemDistance itemDist)
  {
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
   */
  public Object nearestNeighbour(Envelope env, Object item, ItemDistance itemDist)
  {
    Boundable bnd = new ItemBoundable(env, item);
    BoundablePair bp = new BoundablePair(this.getRoot(), bnd, itemDist);
    return nearestNeighbour(bp)[0];
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
   */
  public Envelope[] kNearestNeighbour(Envelope env, Object item, ItemDistance itemDist,int k)
  {
    Boundable bnd = new ItemBoundable(env, item);
    BoundablePair bp = new BoundablePair(this.getRoot(), bnd, itemDist);
    return nearestNeighbour(bp,k);
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
   */
  public Object[] nearestNeighbour(STRtree tree, ItemDistance itemDist)
  {
    BoundablePair bp = new BoundablePair(this.getRoot(), tree.getRoot(), itemDist);
    return nearestNeighbour(bp);
  }
  
  private Object[] nearestNeighbour(BoundablePair initBndPair) 
  {
    return nearestNeighbour(initBndPair, Double.POSITIVE_INFINITY);
  }
  private Envelope[] nearestNeighbour(BoundablePair initBndPair, int k) 
  {
    return nearestNeighbour(initBndPair, Double.POSITIVE_INFINITY,k);
  }
  private Object[] nearestNeighbour(BoundablePair initBndPair, double maxDistance) 
  {
    double distanceLowerBound = maxDistance;
    BoundablePair minPair = null;
    
    // initialize internal structures
    PriorityQueue priQ = new PriorityQueue();

    // initialize queue
    priQ.add(initBndPair);

    while (! priQ.isEmpty() && distanceLowerBound > 0.0) {
      // pop head of queue and expand one side of pair
      BoundablePair bndPair = (BoundablePair) priQ.poll();
      double currentDistance = bndPair.getDistance();
      
      /**
       * If the distance for the first node in the queue
       * is >= the current minimum distance, all other nodes
       * in the queue must also have a greater distance.
       * So the current minDistance must be the true minimum,
       * and we are done.
       */
      if (currentDistance >= distanceLowerBound) 
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
        distanceLowerBound = currentDistance;
        minPair = bndPair;
      }
      else {
        // testing - does allowing a tolerance improve speed?
        // Ans: by only about 10% - not enough to matter
        /*
        double maxDist = bndPair.getMaximumDistance();
        if (maxDist * .99 < lastComputedDistance) 
          return;
        //*/

        /**
         * Otherwise, expand one side of the pair,
         * (the choice of which side to expand is heuristically determined) 
         * and insert the new expanded pairs into the queue
         */
        bndPair.expandToQueue(priQ, distanceLowerBound);
      }
    }
    // done - return items with min distance
    return new Object[] {    
          ((ItemBoundable) minPair.getBoundable(0)).getItem(),
          ((ItemBoundable) minPair.getBoundable(1)).getItem()
      };
  }
  
  private Envelope[] nearestNeighbour(BoundablePair initBndPair, double maxDistance, int k) 
  {
    double distanceLowerBound = maxDistance;
    BoundablePair minPair = null;
    
    // initialize internal structures
    PriorityQueue priQ = new PriorityQueue();

    // initialize queue
    priQ.add(initBndPair);

    List<Envelope> kNearestNeighbors = new ArrayList<Envelope> ();
    List<Double> kNearestDistances = new ArrayList<Double>();
    while (! priQ.isEmpty() && distanceLowerBound >= 0.0) {
      // pop head of queue and expand one side of pair
      BoundablePair bndPair = (BoundablePair) priQ.poll();
      double currentDistance = bndPair.getDistance();
      
      
      /**
       * If the distance for the first node in the queue
       * is >= the current maximum distance in the k queue , all other nodes
       * in the queue must also have a greater distance.
       * So the current minDistance must be the true minimum,
       * and we are done.
       */

      if (currentDistance >= distanceLowerBound && kNearestDistances.size()>=k){
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
    	
        //distanceLowerBound = currentDistance;
    	  if(kNearestDistances.size()>0 && kNearestDistances.size()<k){

    		  int position=Collections.binarySearch(kNearestDistances, currentDistance);
	    	  	if(position<0)
  	            {
  	          	  position=-position-1;
  	            }
	    	  	kNearestNeighbors.add(position,(Envelope)bndPair.getBoundable(0).getBounds());
  	        	kNearestDistances.add(position,currentDistance); 
    	  }
    	  else if(kNearestDistances.size()>=k)
    	  {

  	  		if(currentDistance<kNearestDistances.get(kNearestDistances.size()-1))
  	  		{
  	  			int position=Collections.binarySearch(kNearestDistances, currentDistance);
  	    	  	if(position<0)
  	            {
  	          	  position=-position-1;
  	            }
  	    	  	kNearestNeighbors.add(position,(Envelope)bndPair.getBoundable(0).getBounds());
  	        	kNearestDistances.add(position,currentDistance); 
  	        	assert kNearestNeighbors.size()>k;
  	        	kNearestNeighbors.remove(kNearestNeighbors.size()-1);
  	        	kNearestDistances.remove(kNearestDistances.size()-1);
  	  		}
    	  }
    	  else if(kNearestDistances.size()==0)
    	  {
    		  kNearestNeighbors.add((Envelope)bndPair.getBoundable(0).getBounds());
    		  kNearestDistances.add(currentDistance);
    	  }
    	  else
    	  {
    		  try {
				throw new Exception("Should never reach here");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
    	  }
     	  	   	  
    	 
        distanceLowerBound = kNearestDistances.get(kNearestDistances.size()-1);
        //System.out.println("distanceLowerBound is " + distanceLowerBound);
        
        

      }
      else {
        // testing - does allowing a tolerance improve speed?
        // Ans: by only about 10% - not enough to matter
        /*
        double maxDist = bndPair.getMaximumDistance();
        if (maxDist * .99 < lastComputedDistance) 
          return;
        //*/

        /**
         * Otherwise, expand one side of the pair,
         * (the choice of which side to expand is heuristically determined) 
         * and insert the new expanded pairs into the queue
         */
        bndPair.expandToQueue(priQ, distanceLowerBound);
      }
    }
    // done - return items with min distance

    return kNearestNeighbors.toArray(new Envelope[kNearestNeighbors.size()]);
  }
  /**
   * This method is to find the boundaries of leaf nodes.
   * @return Return the list of boundaries we find.
   */
  public List queryBoundary()
    {

      return super.queryBoundary();


    }

}
