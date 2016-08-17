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
package com.vividsolutions.jts.util;

import java.util.*;

/**
 * A priority queue over a set of {@link Comparable} objects.
 * 
 * @author Martin Davis
 *
 */
public class PriorityQueue 
{
  private int size; // Number of elements in queue
  private ArrayList items; // The queue binary heap array

  /**
   * Creates a new empty priority queue
   */
  public PriorityQueue() {
    size = 0;
    items = new ArrayList();
    // create space for sentinel
    items.add(null);
  }

  /**
   * Insert into the priority queue.
   * Duplicates are allowed.
   * @param x the item to insert.
   */
  public void add(Comparable x) 
  {
    // increase the size of the items heap to create a hole for the new item
    items.add(null);

    // Insert item at end of heap and then re-establish ordering
    size += 1;
    int hole = size;
    // set the item as a sentinel at the base of the heap
    items.set(0, x);

    // move the item up from the hole position to its correct place
    for (; x.compareTo(items.get(hole / 2)) < 0; hole /= 2) {
      items.set(hole, items.get(hole / 2));
    }
    // insert the new item in the correct place
    items.set(hole, x);
  }

  /**
   * Establish heap from an arbitrary arrangement of items. 
   */
  /*
   private void buildHeap( ) {
   for( int i = currentSize / 2; i > 0; i-- )
   reorder( i );
   }
   */

  /**
   * Test if the priority queue is logically empty.
   * @return true if empty, false otherwise.
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns size.
   * @return current size.
   */
  public int size() {
    return size;
  }

  /**
   * Make the priority queue logically empty.
   */
  public void clear() {
    size = 0;
    items.clear();
  }

  /**
   * Remove the smallest item from the priority queue.
   * @return the smallest item, or null if empty
   */
  public Object poll() 
  {
    if (isEmpty())
      return null;
    Object minItem = items.get(1);
    items.set(1, items.get(size));
    size -= 1;
    reorder(1);

    return minItem;
  }


  
  /**
   * Internal method to percolate down in the heap.
   * 
   * @param hole the index at which the percolate begins.
   */
  private void reorder(int hole) 
  {
    int child;
    Object tmp = items.get(hole);

    for (; hole * 2 <= size; hole = child) {
      child = hole * 2;
      if (child != size
          && ((Comparable) items.get(child + 1)).compareTo(items.get(child)) < 0)
        child++;
      if (((Comparable) items.get(child)).compareTo(tmp) < 0)
        items.set(hole, items.get(child));
      else
        break;
    }
    items.set(hole, tmp);
  }
}