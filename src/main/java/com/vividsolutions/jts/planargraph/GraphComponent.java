
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
package com.vividsolutions.jts.planargraph;

import java.util.Iterator;

/**
 * The base class for all graph component classes.
 * Maintains flags of use in generic graph algorithms.
 * Provides two flags:
 * <ul>
 * <li><b>marked</b> - typically this is used to indicate a state that persists
 * for the course of the graph's lifetime.  For instance, it can be
 * used to indicate that a component has been logically deleted from the graph.
 * <li><b>visited</b> - this is used to indicate that a component has been processed
 * or visited by an single graph algorithm.  For instance, a breadth-first traversal of the
 * graph might use this to indicate that a node has already been traversed.
 * The visited flag may be set and cleared many times during the lifetime of a graph.
 *
 * <p>
 * Graph components support storing user context data.  This will typically be
 * used by client algorithms which use planar graphs.
 *
 * @version 1.7
 */
public abstract class GraphComponent
{
  /**
   * Sets the Visited state for all {@link GraphComponent}s in an {@link Iterator}
   *
   * @param i the Iterator to scan
   * @param visited the state to set the visited flag to
   */
  public static void setVisited(Iterator i, boolean visited)
  {
    while (i.hasNext()) {
      GraphComponent comp = (GraphComponent) i.next();
      comp.setVisited(visited);
    }
  }

  /**
   * Sets the Marked state for all {@link GraphComponent}s in an {@link Iterator}
   *
   * @param i the Iterator to scan
   * @param marked the state to set the Marked flag to
   */
  public static void setMarked(Iterator i, boolean marked)
  {
    while (i.hasNext()) {
      GraphComponent comp = (GraphComponent) i.next();
      comp.setMarked(marked);
    }
  }

  /**
   * Finds the first {@link GraphComponent} in a {@link Iterator} set
   * which has the specified visited state.
   *
   * @param i an Iterator of GraphComponents
   * @param visitedState the visited state to test
   * @return the first component found, or <code>null</code> if none found
   */
  public static GraphComponent getComponentWithVisitedState(Iterator i, boolean visitedState)
  {
    while (i.hasNext()) {
      GraphComponent comp = (GraphComponent) i.next();
      if (comp.isVisited() == visitedState)
        return comp;
    }
    return null;
  }

  protected boolean isMarked = false;
  protected boolean isVisited = false;
  private Object data;

  public GraphComponent() {
  }

  /**
   * Tests if a component has been visited during the course of a graph algorithm
   * @return <code>true</code> if the component has been visited
   */
  public boolean isVisited() { return isVisited; }

  /**
   * Sets the visited flag for this component.
   * @param isVisited the desired value of the visited flag
   */
  public void setVisited(boolean isVisited) { this.isVisited = isVisited; }

  /**
   * Tests if a component has been marked at some point during the processing
   * involving this graph.
   * @return <code>true</code> if the component has been marked
   */
  public boolean isMarked() { return isMarked; }

  /**
   * Sets the marked flag for this component.
   * @param isMarked the desired value of the marked flag
   */
  public void setMarked(boolean isMarked) { this.isMarked = isMarked; }

  /**
   * Sets the user-defined data for this component.
   *
   * @param data an Object containing user-defined data
   */
  public void setContext(Object data) { this.data = data; }

  /**
   * Gets the user-defined data for this component.
   *
   * @return the user-defined data
   */
  public Object getContext() { return data; }

  /**
   * Sets the user-defined data for this component.
   *
   * @param data an Object containing user-defined data
   */
  public void setData(Object data) { this.data = data; }

  /**
   * Gets the user-defined data for this component.
   *
   * @return the user-defined data
   */
  public Object getData() { return data; }

  /**
   * Tests whether this component has been removed from its containing graph
   *
   * @return <code>true</code> if this component is removed
   */
  public abstract boolean isRemoved();
}
