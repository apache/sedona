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
 * Utilities for processing {@link Collection}s.
 *
 * @version 1.7
 */
public class CollectionUtil 
{

  public interface Function {
    Object execute(Object obj);
  }

  /**
   * Executes a function on each item in a {@link Collection}
   * and returns the results in a new {@link List}
   *
   * @param coll the collection to process
   * @param func the Function to execute
   * @return a list of the transformed objects
   */
  public static List transform(Collection coll, Function func)
  {
    List result = new ArrayList();
    for (Iterator i = coll.iterator(); i.hasNext(); ) {
      result.add(func.execute(i.next()));
    }
    return result;
  }

  /**
   * Executes a function on each item in a Collection but does
   * not accumulate the result
   *
   * @param coll the collection to process
   * @param func the Function to execute
   */
  public static void apply(Collection coll, Function func)
  {
    for (Iterator i = coll.iterator(); i.hasNext(); ) {
      func.execute(i.next());
    }
  }

  /**
   * Executes a {@link Function} on each item in a Collection
   * and collects all the entries for which the result
   * of the function is equal to {@link Boolean} <tt>true</tt>.
   *
   * @param collection the collection to process
   * @param func the Function to execute
   * @return a list of objects for which the function was true
   */
  public static List select(Collection collection, Function func) {
    List result = new ArrayList();
    for (Iterator i = collection.iterator(); i.hasNext();) {
      Object item = i.next();
      if (Boolean.TRUE.equals(func.execute(item))) {
        result.add(item);
      }
    }
    return result;
  }
}
