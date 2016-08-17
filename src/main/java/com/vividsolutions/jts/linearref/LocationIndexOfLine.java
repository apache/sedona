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
 * Determines the location of a subline along a linear {@link Geometry}.
 * The location is reported as a pair of {@link LinearLocation}s.
 * <p>
 * <b>Note:</b> Currently this algorithm is not guaranteed to
 * return the correct substring in some situations where
 * an endpoint of the test line occurs more than once in the input line.
 * (However, the common case of a ring is always handled correctly).
 */
class LocationIndexOfLine
{
  /**
  * MD - this algorithm has been extracted into a class
  * because it is intended to validate that the subline truly is a subline,
  * and also to use the internal vertex information to unambiguously locate the subline.
  */
 public static LinearLocation[] indicesOf(Geometry linearGeom, Geometry subLine)
  {
    LocationIndexOfLine locater = new LocationIndexOfLine(linearGeom);
    return locater.indicesOf(subLine);
  }

  private Geometry linearGeom;

  public LocationIndexOfLine(Geometry linearGeom) {
    this.linearGeom = linearGeom;
  }

  public LinearLocation[] indicesOf(Geometry subLine)
  {
    Coordinate startPt = ((LineString) subLine.getGeometryN(0)).getCoordinateN(0);
    LineString lastLine = (LineString) subLine.getGeometryN(subLine.getNumGeometries() - 1);
    Coordinate endPt = lastLine.getCoordinateN(lastLine.getNumPoints() - 1);

    LocationIndexOfPoint locPt = new LocationIndexOfPoint(linearGeom);
    LinearLocation[] subLineLoc = new LinearLocation[2];
    subLineLoc[0] = locPt.indexOf(startPt);

    // check for case where subline is zero length
    if (subLine.getLength() == 0.0) {
      subLineLoc[1] = (LinearLocation) subLineLoc[0].clone();
    }
    else  {
      subLineLoc[1] = locPt.indexOfAfter(endPt, subLineLoc[0]);
    }
    return subLineLoc;
  }
}
