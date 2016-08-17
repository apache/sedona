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

package com.vividsolutions.jts.linearref;

import com.vividsolutions.jts.geom.*;

/**
 * An iterator over the components and coordinates of a linear geometry
 * ({@link LineString}s and {@link MultiLineString}s.
 *
 * The standard usage pattern for a {@link LinearIterator} is:
 *
 * <pre>
 * for (LinearIterator it = new LinearIterator(...); it.hasNext(); it.next()) {
 *   ...
 *   int ci = it.getComponentIndex();   // for example
 *   int vi = it.getVertexIndex();      // for example
 *   ...
 * }
 * </pre>
 *
 * @version 1.7
 */
public class LinearIterator
{
  private static int segmentEndVertexIndex(LinearLocation loc)
  {
    if (loc.getSegmentFraction() > 0.0)
      return loc.getSegmentIndex() + 1;
    return loc.getSegmentIndex();
  }

  private Geometry linearGeom;
  private final int numLines;

  /**
   * Invariant: currentLine <> null if the iterator is pointing at a valid coordinate
   */
  private LineString currentLine;
  private int componentIndex = 0;
  private int vertexIndex = 0;

  /**
   * Creates an iterator initialized to the start of a linear {@link Geometry}
   *
   * @param linear the linear geometry to iterate over
   * @throws IllegalArgumentException if linearGeom is not lineal
   */
  public LinearIterator(Geometry linear) {
    this(linear, 0, 0);
  }

  /**
   * Creates an iterator starting at
   * a {@link LinearLocation} on a linear {@link Geometry}
   *
   * @param linear the linear geometry to iterate over
   * @param start the location to start at
   * @throws IllegalArgumentException if linearGeom is not lineal
   */
  public LinearIterator(Geometry linear, LinearLocation start) {
    this(linear, start.getComponentIndex(), segmentEndVertexIndex(start));
  }

  /**
   * Creates an iterator starting at
   * a specified component and vertex in a linear {@link Geometry}
   *
   * @param linearGeom the linear geometry to iterate over
   * @param componentIndex the component to start at
   * @param vertexIndex the vertex to start at
   * @throws IllegalArgumentException if linearGeom is not lineal
   */
  public LinearIterator(Geometry linearGeom, int componentIndex, int vertexIndex) 
  {
  	if (! (linearGeom instanceof Lineal))
  			throw new IllegalArgumentException("Lineal geometry is required");
    this.linearGeom = linearGeom;
    numLines = linearGeom.getNumGeometries();
    this.componentIndex = componentIndex;
    this.vertexIndex = vertexIndex;
    loadCurrentLine();
  }

  private void loadCurrentLine()
  {
    if (componentIndex >= numLines) {
      currentLine = null;
      return;
    }
    currentLine = (LineString) linearGeom.getGeometryN(componentIndex);
  }

  /**
   * Tests whether there are any vertices left to iterator over.
   * Specifically, hasNext() return <tt>true</tt> if the
   * current state of the iterator represents a valid location
   * on the linear geometry. 
   * 
   * @return <code>true</code> if there are more vertices to scan
   */
  public boolean hasNext()
  {
    if (componentIndex >= numLines) return false;
    if (componentIndex == numLines - 1
        && vertexIndex >= currentLine.getNumPoints())
      return false;
    return true;
  }

  /**
   * Moves the iterator ahead to the next vertex and (possibly) linear component.
   */
  public void next()
  {
    if (! hasNext()) return;

    vertexIndex++;
    if (vertexIndex >= currentLine.getNumPoints()) {
      componentIndex++;
      loadCurrentLine();
      vertexIndex = 0;
    }
  }

  /**
   * Checks whether the iterator cursor is pointing to the
   * endpoint of a component {@link LineString}.
   *
   * @return <code>true</true> if the iterator is at an endpoint
   */
  public boolean isEndOfLine() {
    if (componentIndex >= numLines) return false;
    //LineString currentLine = (LineString) linear.getGeometryN(componentIndex);
    if (vertexIndex < currentLine.getNumPoints() - 1)
      return false;
    return true;
  }

  /**
   * The component index of the vertex the iterator is currently at.
   * @return the current component index
   */
  public int getComponentIndex() { return componentIndex; }

  /**
   * The vertex index of the vertex the iterator is currently at.
   * @return the current vertex index
   */
  public int getVertexIndex() { return vertexIndex; }

  /**
   * Gets the {@link LineString} component the iterator is current at.
   * @return a linestring
   */
  public LineString getLine()  {    return currentLine;  }

  /**
   * Gets the first {@link Coordinate} of the current segment.
   * (the coordinate of the current vertex).
   * @return a {@link Coordinate}
   */
  public Coordinate getSegmentStart() { return currentLine.getCoordinateN(vertexIndex); }

  /**
   * Gets the second {@link Coordinate} of the current segment.
   * (the coordinate of the next vertex).
   * If the iterator is at the end of a line, <code>null</code> is returned.
   *
   * @return a {@link Coordinate} or <code>null</code>
   */
  public Coordinate getSegmentEnd()
  {
    if (vertexIndex < getLine().getNumPoints() - 1)
      return currentLine.getCoordinateN(vertexIndex + 1);
    return null;
  }
}
