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
 * Supports linear referencing along a linear {@link Geometry}
 * using the length along the line as the index.
 * Negative length values are taken as measured in the reverse direction
 * from the end of the geometry.
 * Out-of-range index values are handled by clamping
 * them to the valid range of values.
 * Non-simple lines (i.e. which loop back to cross or touch
 * themselves) are supported.
 */
public class LengthIndexedLine
{
  private Geometry linearGeom;

  /**
   * Constructs an object which allows a linear {@link Geometry}
   * to be linearly referenced using length as an index.
   *
   * @param linearGeom the linear geometry to reference along
   */
  public LengthIndexedLine(Geometry linearGeom) {
    this.linearGeom = linearGeom;
  }

  /**
   * Computes the {@link Coordinate} for the point
   * on the line at the given index.
   * If the index is out of range the first or last point on the
   * line will be returned.
   * The Z-ordinate of the computed point will be interpolated from
   * the Z-ordinates of the line segment containing it, if they exist.
   *
   * @param index the index of the desired point
   * @return the Coordinate at the given index
   */
  public Coordinate extractPoint(double index)
  {
    LinearLocation loc = LengthLocationMap.getLocation(linearGeom, index);
    return loc.getCoordinate(linearGeom);
  }

  /**
   * Computes the {@link Coordinate} for the point
   * on the line at the given index, offset by the given distance.
   * If the index is out of range the first or last point on the
   * line will be returned.
   * The computed point is offset to the left of the line if the offset distance is
   * positive, to the right if negative.
   * 
   * The Z-ordinate of the computed point will be interpolated from
   * the Z-ordinates of the line segment containing it, if they exist.
   *
   * @param index the index of the desired point
   * @param offsetDistance the distance the point is offset from the segment
   *    (positive is to the left, negative is to the right)
   * @return the Coordinate at the given index
   */
  public Coordinate extractPoint(double index, double offsetDistance)
  {
    LinearLocation loc = LengthLocationMap.getLocation(linearGeom, index);
    LinearLocation locLow = loc.toLowest(linearGeom);
    return locLow.getSegment(linearGeom).pointAlongOffset(locLow.getSegmentFraction(), offsetDistance);
  }

  /**
   * Computes the {@link LineString} for the interval
   * on the line between the given indices.
   * If the endIndex lies before the startIndex,
   * the computed geometry is reversed.
   *
   * @param startIndex the index of the start of the interval
   * @param endIndex the index of the end of the interval
   * @return the linear interval between the indices
   */
  public Geometry extractLine(double startIndex, double endIndex)
  {
    LocationIndexedLine lil = new LocationIndexedLine(linearGeom);
    double startIndex2 = clampIndex(startIndex);
    double endIndex2 = clampIndex(endIndex);
    // if extracted line is zero-length, resolve start lower as well to ensure they are equal
    boolean resolveStartLower = startIndex2 == endIndex2;
    LinearLocation startLoc = locationOf(startIndex2, resolveStartLower);
//    LinearLocation endLoc = locationOf(endIndex2, true);
//    LinearLocation startLoc = locationOf(startIndex2);
    LinearLocation endLoc = locationOf(endIndex2);
    return ExtractLineByLocation.extract(linearGeom, startLoc, endLoc);
  }

  private LinearLocation locationOf(double index)
  {
    return LengthLocationMap.getLocation(linearGeom, index);
  }

  private LinearLocation locationOf(double index, boolean resolveLower)
  {
    return LengthLocationMap.getLocation(linearGeom, index, resolveLower);
  }

  /**
   * Computes the minimum index for a point on the line.
   * If the line is not simple (i.e. loops back on itself)
   * a single point may have more than one possible index.
   * In this case, the smallest index is returned.
   *
   * The supplied point does not <i>necessarily</i> have to lie precisely
   * on the line, but if it is far from the line the accuracy and
   * performance of this function is not guaranteed.
   * Use {@link #project} to compute a guaranteed result for points
   * which may be far from the line.
   *
   * @param pt a point on the line
   * @return the minimum index of the point
   *
   * @see #project(Coordinate)
   */
  public double indexOf(Coordinate pt)
  {
    return LengthIndexOfPoint.indexOf(linearGeom, pt);
  }

  /**
   * Finds the index for a point on the line
   * which is greater than the given index.
   * If no such index exists, returns <tt>minIndex</tt>.
   * This method can be used to determine all indexes for
   * a point which occurs more than once on a non-simple line.
   * It can also be used to disambiguate cases where the given point lies
   * slightly off the line and is equidistant from two different
   * points on the line.
   *
   * The supplied point does not <i>necessarily</i> have to lie precisely
   * on the line, but if it is far from the line the accuracy and
   * performance of this function is not guaranteed.
   * Use {@link #project} to compute a guaranteed result for points
   * which may be far from the line.
   *
   * @param pt a point on the line
   * @param minIndex the value the returned index must be greater than
   * @return the index of the point greater than the given minimum index
   *
   * @see #project(Coordinate)
   */
  public double indexOfAfter(Coordinate pt, double minIndex)
  {
    return LengthIndexOfPoint.indexOfAfter(linearGeom, pt, minIndex);
  }

  /**
   * Computes the indices for a subline of the line.
   * (The subline must <b>conform</b> to the line; that is,
   * all vertices in the subline (except possibly the first and last)
   * must be vertices of the line and occcur in the same order).
   *
   * @param subLine a subLine of the line
   * @return a pair of indices for the start and end of the subline.
   */
  public double[] indicesOf(Geometry subLine)
  {
    LinearLocation[] locIndex = LocationIndexOfLine.indicesOf(linearGeom, subLine);
    double[] index = new double[] {
      LengthLocationMap.getLength(linearGeom, locIndex[0]),
      LengthLocationMap.getLength(linearGeom, locIndex[1])
      };
    return index;
  }


  /**
   * Computes the index for the closest point on the line to the given point.
   * If more than one point has the closest distance the first one along the line
   * is returned.
   * (The point does not necessarily have to lie precisely on the line.)
   *
   * @param pt a point on the line
   * @return the index of the point
   */
  public double project(Coordinate pt)
  {
    return LengthIndexOfPoint.indexOf(linearGeom, pt);
  }

  /**
   * Returns the index of the start of the line
   * @return the start index
   */
  public double getStartIndex()
  {
    return 0.0;
  }

  /**
   * Returns the index of the end of the line
   * @return the end index
   */
  public double getEndIndex()
  {
    return linearGeom.getLength();
  }

  /**
   * Tests whether an index is in the valid index range for the line.
   *
   * @param index the index to test
   * @return <code>true</code> if the index is in the valid range
   */
  public boolean isValidIndex(double index)
  {
    return (index >= getStartIndex()
            && index <= getEndIndex());
  }

  /**
   * Computes a valid index for this line
   * by clamping the given index to the valid range of index values
   *
   * @return a valid index value
   */
  public double clampIndex(double index)
  {
    double posIndex = positiveIndex(index);
    double startIndex = getStartIndex();
    if (posIndex < startIndex) return startIndex;

    double endIndex = getEndIndex();
    if (posIndex > endIndex) return endIndex;

    return posIndex;
  }
  
  private double positiveIndex(double index)
  {
    if (index >= 0.0) return index;
    return linearGeom.getLength() + index;
  }
}
