

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
package com.vividsolutions.jts.geom;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *  Iterates over all {@link Geometry}s in a {@link Geometry},
 *  (which may be either a collection or an atomic geometry).
 *  The iteration sequence follows a pre-order, depth-first traversal of the 
 *  structure of the <code>GeometryCollection</code>
 *  (which may be nested). The original <code>Geometry</code> object is
 *  returned as well (as the first object), as are all sub-collections and atomic elements. 
 *  It is  simple to ignore the intermediate <code>GeometryCollection</code> objects if they are not
 *  needed.
 *
 *@version 1.7
 */
public class GeometryCollectionIterator implements Iterator {

  /**
   *  The <code>Geometry</code> being iterated over.
   */
  private Geometry parent;
  /**
   *  Indicates whether or not the first element 
   *  (the root <code>GeometryCollection</code>) has been returned.
   */
  private boolean atStart;
  /**
   *  The number of <code>Geometry</code>s in the the <code>GeometryCollection</code>.
   */
  private int max;
  /**
   *  The index of the <code>Geometry</code> that will be returned when <code>next</code>
   *  is called.
   */
  private int index;
  /**
   *  The iterator over a nested <code>Geometry</code>, or <code>null</code>
   *  if this <code>GeometryCollectionIterator</code> is not currently iterating
   *  over a nested <code>GeometryCollection</code>.
   */
  private GeometryCollectionIterator subcollectionIterator;

  /**
   *  Constructs an iterator over the given <code>Geometry</code>.
   *
   *@param  parent  the geometry over which to iterate; also, the first
   *      element returned by the iterator.
   */
  public GeometryCollectionIterator(Geometry parent) {
    this.parent = parent;
    atStart = true;
    index = 0;
    max = parent.getNumGeometries();
  }

  /**
   * Tests whether any geometry elements remain to be returned.
   * 
   * @return true if more geometry elements remain
   */
  public boolean hasNext() {
    if (atStart) {
      return true;
    }
    if (subcollectionIterator != null) {
      if (subcollectionIterator.hasNext()) {
        return true;
      }
      subcollectionIterator = null;
    }
    if (index >= max) {
      return false;
    }
    return true;
  }

  /**
   * Gets the next geometry in the iteration sequence.
   * 
   * @return the next geometry in the iteration
   */
  public Object next() {
    // the parent GeometryCollection is the first object returned
    if (atStart) {
      atStart = false;
      if (isAtomic(parent))
        index++;
      return parent;
    }
    if (subcollectionIterator != null) {
      if (subcollectionIterator.hasNext()) {
        return subcollectionIterator.next();
      }
      else {
        subcollectionIterator = null;
      }
    }
    if (index >= max) {
      throw new NoSuchElementException();
    }
    Geometry obj = parent.getGeometryN(index++);
    if (obj instanceof GeometryCollection) {
      subcollectionIterator = new GeometryCollectionIterator((GeometryCollection) obj);
      // there will always be at least one element in the sub-collection
      return subcollectionIterator.next();
    }
    return obj;
  }

  private static boolean isAtomic(Geometry geom)
  {
    return ! (geom instanceof GeometryCollection);
  }
  
  /**
   * Removal is not supported.
   *
   * @throws  UnsupportedOperationException  This method is not implemented.
   */
  public void remove() {
    throw new UnsupportedOperationException(getClass().getName());
  }
}

