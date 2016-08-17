
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
package com.vividsolutions.jts.index.bintree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.geom.Envelope;

/**
 * An <code>BinTree</code> (or "Binary Interval Tree")
 * is a 1-dimensional version of a quadtree.
 * It indexes 1-dimensional intervals (which may
 * be the projection of 2-D objects on an axis).
 * It supports range searching
 * (where the range may be a single point).
 * This structure is dynamic - 
 * new items can be added at any time,   
 * and it will support deletion of items 
 * (although this is not currently implemented).
 * <p>
 * This implementation does not require specifying the extent of the inserted
 * items beforehand.  It will automatically expand to accomodate any extent
 * of dataset.
 * <p>
 * The bintree structure is used to provide a primary filter
 * for interval queries.  The query() method returns a list of
 * all objects which <i>may</i> intersect the query interval.
 * Note that it may return objects which do not in fact intersect.
 * A secondary filter is required to test for exact intersection.
 * Of course, this secondary filter may consist of other tests besides
 * intersection, such as testing other kinds of spatial relationships.
 * <p>
 * This index is different to the Interval Tree of Edelsbrunner
 * or the Segment Tree of Bentley.
 *
 * @version 1.7
 */
public class Bintree
{
  /**
   * Ensure that the Interval for the inserted item has non-zero extents.
   * Use the current minExtent to pad it, if necessary
   */
  public static Interval ensureExtent(Interval itemInterval, double minExtent)
  {
    double min = itemInterval.getMin();
    double max = itemInterval.getMax();
    // has a non-zero extent
    if (min != max) return itemInterval;

    // pad extent
    if (min == max) {
      min = min - minExtent / 2.0;
      max = min + minExtent / 2.0;
    }
    return new Interval(min, max);
  }

  private Root root;
  /**
  *  Statistics
  *
  * minExtent is the minimum extent of all items
  * inserted into the tree so far. It is used as a heuristic value
  * to construct non-zero extents for features with zero extent.
  * Start with a non-zero extent, in case the first feature inserted has
  * a zero extent in both directions.  This value may be non-optimal, but
  * only one feature will be inserted with this value.
  **/
  private double minExtent = 1.0;

  public Bintree()
  {
    root = new Root();
  }

  public int depth()
  {
    if (root != null) return root.depth();
    return 0;
  }
  public int size()
  {
    if (root != null) return root.size();
    return 0;
  }
  /**
   * Compute the total number of nodes in the tree
   *
   * @return the number of nodes in the tree
   */
  public int nodeSize()
  {
    if (root != null) return root.nodeSize();
    return 0;
  }

  public void insert(Interval itemInterval, Object item)
  {
    collectStats(itemInterval);
    Interval insertInterval = ensureExtent(itemInterval, minExtent);
//int oldSize = size();
    root.insert(insertInterval, item);
    /* DEBUG
int newSize = size();
System.out.println("BinTree: size = " + newSize + "   node size = " + nodeSize());
if (newSize <= oldSize) {
      System.out.println("Lost item!");
      root.insert(insertInterval, item);
      System.out.println("reinsertion size = " + size());
}
    */
  }

  /**
   * Removes a single item from the tree.
   *
   * @param itemEnv the Envelope of the item to be removed
   * @param item the item to remove
   * @return <code>true</code> if the item was found (and thus removed)
   */
  public boolean remove(Interval itemInterval, Object item)
  {
    Interval insertInterval = ensureExtent(itemInterval, minExtent);
    return root.remove(insertInterval, item);
  }
  
  public Iterator iterator()
  {
    List foundItems = new ArrayList();
    root.addAllItems(foundItems);
    return foundItems.iterator();
  }

  public List query(double x)
  {
    return query(new Interval(x, x));
  }

  /**
   * Queries the tree to find all candidate items which 
   * may overlap the query interval.
   * If the query interval is <tt>null</tt>, all items in the tree are found.
   * 
   * min and max may be the same value
   */
  public List query(Interval interval)
  {
    /**
     * the items that are matched are all items in intervals
     * which overlap the query interval
     */
    List foundItems = new ArrayList();
    query(interval, foundItems);
    return foundItems;
  }

  /**
   * Adds items in the tree which potentially overlap the query interval
   * to the given collection.
   * If the query interval is <tt>null</tt>, add all items in the tree.
   * 
   * @param interval a query nterval, or null
   * @param resultItems the candidate items found
   */
  public void query(Interval interval, Collection foundItems)
  {
    root.addAllItemsFromOverlapping(interval, foundItems);
  }

  private void collectStats(Interval interval)
  {
    double del = interval.getWidth();
    if (del < minExtent && del > 0.0)
      minExtent = del;
  }

}
