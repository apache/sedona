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
import com.vividsolutions.jts.util.Assert;

/**
 * Extracts the subline of a linear {@link Geometry} between
 * two {@link LinearLocation}s on the line.
 */
class ExtractLineByLocation
{
  /**
   * Computes the subline of a {@link LineString} between
   * two {@link LinearLocation}s on the line.
   * If the start location is after the end location,
   * the computed linear geometry has reverse orientation to the input line.
   *
   * @param line the line to use as the baseline
   * @param start the start location
   * @param end the end location
   * @return the extracted subline
   */
  public static Geometry extract(Geometry line, LinearLocation start, LinearLocation end)
  {
    ExtractLineByLocation ls = new ExtractLineByLocation(line);
    return ls.extract(start, end);
  }

  private Geometry line;

  public ExtractLineByLocation(Geometry line) {
    this.line = line;
  }

  /**
   * Extracts a subline of the input.
   * If <code>end < start</code> the linear geometry computed will be reversed.
   *
   * @param start the start location
   * @param end the end location
   * @return a linear geometry
   */
  public Geometry extract(LinearLocation start, LinearLocation end)
  {
    if (end.compareTo(start) < 0) {
      return reverse(computeLinear(end, start));
    }
    return computeLinear(start, end);
  }

  private Geometry reverse(Geometry linear)
  {
    if (linear instanceof LineString)
      return ((LineString) linear).reverse();
    if (linear instanceof MultiLineString)
      return ((MultiLineString) linear).reverse();
    Assert.shouldNeverReachHere("non-linear geometry encountered");
    return null;
  }
  /**
   * Assumes input is valid (e.g. start <= end)
   *
   * @param start
   * @param end
   * @return a linear geometry
   */
  private LineString computeLine(LinearLocation start, LinearLocation end)
  {
    Coordinate[] coordinates = line.getCoordinates();
    CoordinateList newCoordinates = new CoordinateList();

    int startSegmentIndex = start.getSegmentIndex();
    if (start.getSegmentFraction() > 0.0)
      startSegmentIndex += 1;
    int lastSegmentIndex = end.getSegmentIndex();
    if (end.getSegmentFraction() == 1.0)
      lastSegmentIndex += 1;
    if (lastSegmentIndex >= coordinates.length)
      lastSegmentIndex = coordinates.length - 1;
    // not needed - LinearLocation values should always be correct
    //Assert.isTrue(end.getSegmentFraction() <= 1.0, "invalid segment fraction value");

    if (! start.isVertex())
      newCoordinates.add(start.getCoordinate(line));
    for (int i = startSegmentIndex; i <= lastSegmentIndex; i++) {
      newCoordinates.add(coordinates[i]);
    }
    if (! end.isVertex())
      newCoordinates.add(end.getCoordinate(line));

    // ensure there is at least one coordinate in the result
    if (newCoordinates.size() <= 0)
      newCoordinates.add(start.getCoordinate(line));

    Coordinate[] newCoordinateArray = newCoordinates.toCoordinateArray();
    /**
     * Ensure there is enough coordinates to build a valid line.
     * Make a 2-point line with duplicate coordinates, if necessary.
     * There will always be at least one coordinate in the coordList.
     */
    if (newCoordinateArray.length <= 1) {
      newCoordinateArray = new Coordinate[] { newCoordinateArray[0], newCoordinateArray[0]};
    }
    return line.getFactory().createLineString(newCoordinateArray);
  }

  /**
   * Assumes input is valid (e.g. start <= end)
   *
   * @param start
   * @param end
   * @return a linear geometry
   */
  private Geometry computeLinear(LinearLocation start, LinearLocation end)
  {
    LinearGeometryBuilder builder = new LinearGeometryBuilder(line.getFactory());
    builder.setFixInvalidLines(true);

    if (! start.isVertex())
      builder.add(start.getCoordinate(line));

    for (LinearIterator it = new LinearIterator(line, start); it.hasNext(); it.next()) {
      if (end.compareLocationValues(it.getComponentIndex(), it.getVertexIndex(), 0.0)
        < 0)
        break;

      Coordinate pt = it.getSegmentStart();
      builder.add(pt);
      if (it.isEndOfLine())
        builder.endLine();
    }
    if (! end.isVertex())
      builder.add(end.getCoordinate(line));

    return builder.getGeometry();
  }

  /**
   * Computes a valid and normalized location
   * compatible with the values in a LinearIterator.
   * (I.e. segmentFractions of 1.0 are converted to the next highest coordinate index)
   */
  /*
  private LinearLocation normalize(LinearLocation loc)
  {
    int componentIndex = loc.getComponentIndex();
    int segmentIndex = loc.getSegmentIndex();
    double segmentFraction = loc.getSegmentFraction();

    if (segmentFraction < 0.0) {
      segmentFraction = 0.0;
    }
    if (segmentFraction > 1.0) {
      segmentFraction = 1.0;
    }

    if (componentIndex < 0) {
      componentIndex = 0;
      segmentIndex = 0;
      segmentFraction = 0.0;
    }
    if (segmentIndex < 0) {
      segmentIndex = 0;
      segmentFraction = 0.0;
    }

    if (segmentFraction == 1.0) {
      segmentFraction = 0.0;
      segmentIndex += 1;
    }

    return new LinearLocation(componentIndex, segmentIndex, segmentFraction);
  }
  */
}
