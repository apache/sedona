/*
 * Copyright (c) 2016 Martin Davis.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.apache.sedona.jts.index.strtree;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.locationtech.jts.geom.Envelope;


/**
 * A pair of {@link Boundable}s, whose leaf items 
 * support a distance metric between them.
 * Used to compute the distance between the members,
 * and to expand a member relative to the other
 * in order to produce new branches of the 
 * Branch-and-Bound evaluation tree.
 * Provides an ordering based on the distance between the members,
 * which allows building a priority queue by minimum distance.
 * 
 * @author Martin Davis
 *
 */
class BoundablePair
  implements Comparable
{
  private Boundable boundable1;
  private Boundable boundable2;
  private double distance;
  private ItemDistance itemDistance;
  //private double maxDistance = -1.0;
  
  public BoundablePair(Boundable boundable1, Boundable boundable2, ItemDistance itemDistance)
  {
    this.boundable1 = boundable1;
    this.boundable2 = boundable2;
    this.itemDistance = itemDistance;
    distance = distance();
  }
  
  /**
   * Gets one of the member {@link Boundable}s in the pair 
   * (indexed by [0, 1]).
   * 
   * @param i the index of the member to return (0 or 1)
   * @return the chosen member
   */
  public Boundable getBoundable(int i)
  {
    if (i == 0) return boundable1;
    return boundable2;
  }

  /**
   * Computes the maximum distance between any
   * two items in the pair of nodes.
   * 
   * @return the maximum distance between items in the pair
   */
  public double maximumDistance()
  {
    return EnvelopeDistance.maximumDistance( 
        (Envelope) boundable1.getBounds(),
        (Envelope) boundable2.getBounds());       
  }
  
  /**
   * Computes the distance between the {@link Boundable}s in this pair.
   * The boundables are either composites or leaves.
   * If either is composite, the distance is computed as the minimum distance
   * between the bounds.  
   * If both are leaves, the distance is computed by {@link #itemDistance(ItemBoundable, ItemBoundable)}.
   * 
   * @return
   */
  private double distance()
  {
    // if items, compute exact distance
    if (isLeaves()) {
      return itemDistance.distance((ItemBoundable) boundable1,
          (ItemBoundable) boundable2);
    }
    // otherwise compute distance between bounds of boundables
    return ((Envelope) boundable1.getBounds()).distance(
        ((Envelope) boundable2.getBounds()));
  }
  
  /**
   * Gets the minimum possible distance between the Boundables in
   * this pair. 
   * If the members are both items, this will be the
   * exact distance between them.
   * Otherwise, this distance will be a lower bound on 
   * the distances between the items in the members.
   * 
   * @return the exact or lower bound distance for this pair
   */
  public double getDistance() { return distance; }
  
  /**
   * Compares two pairs based on their minimum distances
   */
  public int compareTo(Object o)
  {
    BoundablePair nd = (BoundablePair) o;
    if (distance < nd.distance) return -1;
    if (distance > nd.distance) return 1;
    return 0;
  }

  /**
   * Tests if both elements of the pair are leaf nodes
   * 
   * @return true if both pair elements are leaf nodes
   */
  public boolean isLeaves()
  {
    return ! (isComposite(boundable1) || isComposite(boundable2));
  }
  
  public static boolean isComposite(Object item)
  {
    return (item instanceof AbstractNode); 
  }
  
  private static double area(Boundable b)
  {
    return ((Envelope) b.getBounds()).getArea();
  }
  
  /**
   * For a pair which is not a leaf 
   * (i.e. has at least one composite boundable)
   * computes a list of new pairs 
   * from the expansion of the larger boundable
   * with distance less than minDistance
   * and adds them to a priority queue.
   * <p>
   * Note that expanded pairs may contain
   * the same item/node on both sides.
   * This must be allowed to support distance
   * functions which have non-zero distances
   * between the item and itself (non-zero reflexive distance).
   * 
   * @param priQ the priority queue to add the new pairs to
   * @param minDistance the limit on the distance between added pairs
   * 
   */
  public void expandToQueue(PriorityQueue priQ, double minDistance)
  {
    boolean isComp1 = isComposite(boundable1);
    boolean isComp2 = isComposite(boundable2);
    
    /**
     * HEURISTIC: If both boundable are composite,
     * choose the one with largest area to expand.
     * Otherwise, simply expand whichever is composite.
     */
    if (isComp1 && isComp2) {
      if (area(boundable1) > area(boundable2)) {
        expand(boundable1, boundable2, false, priQ, minDistance);
        return;
      }
      else {
        expand(boundable2, boundable1, true, priQ, minDistance);
        return;
      }
    }
    else if (isComp1) {
      expand(boundable1, boundable2, false, priQ, minDistance);
      return;
    }
    else if (isComp2) {
      expand(boundable2, boundable1, true, priQ, minDistance);
      return;
    }
    
    throw new IllegalArgumentException("neither boundable is composite");
  }
  
  private void expand(Boundable bndComposite, Boundable bndOther, boolean isFlipped,
      PriorityQueue priQ, double minDistance)
  {
    List children = ((AbstractNode) bndComposite).getChildBoundables();
    for (Iterator i = children.iterator(); i.hasNext(); ) {
      Boundable child = (Boundable) i.next();
      BoundablePair bp;
      if (isFlipped) {
        bp = new BoundablePair(bndOther, child, itemDistance);
      }
      else {
        bp = new BoundablePair(child, bndOther, itemDistance);        
      }
      // only add to queue if this pair might contain the closest points
      // MD - it's actually faster to construct the object rather than called distance(child, bndOther)!
      if (bp.getDistance() < minDistance) {
        priQ.add(bp);
      }
    }
  }
}
