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

package com.vividsolutions.jts.simplify;

import java.util.*;
import com.vividsolutions.jts.geom.*;

/**
 * Represents a {@link LineString} which can be modified to a simplified shape.  
 * This class provides an attribute which specifies the minimum allowable length
 * for the modified result.
 * 
 * @version 1.7
 */
class TaggedLineString
{

  private LineString parentLine;
  private TaggedLineSegment[] segs;
  private List resultSegs = new ArrayList();
  private int minimumSize;

  public TaggedLineString(LineString parentLine) {
    this(parentLine, 2);
  }

  public TaggedLineString(LineString parentLine, int minimumSize) {
    this.parentLine = parentLine;
    this.minimumSize = minimumSize;
    init();
  }

  public int getMinimumSize()  {    return minimumSize;  }
  public LineString getParent() { return parentLine; }
  public Coordinate[] getParentCoordinates() { return parentLine.getCoordinates(); }
  public Coordinate[] getResultCoordinates() { return extractCoordinates(resultSegs); }

  public int getResultSize()
  {
    int resultSegsSize = resultSegs.size();
    return resultSegsSize == 0 ? 0 : resultSegsSize + 1;
  }

  public TaggedLineSegment getSegment(int i) { return segs[i]; }

  private void init()
  {
    Coordinate[] pts = parentLine.getCoordinates();
    segs = new TaggedLineSegment[pts.length - 1];
    for (int i = 0; i < pts.length - 1; i++) {
      TaggedLineSegment seg
               = new TaggedLineSegment(pts[i], pts[i + 1], parentLine, i);
      segs[i] = seg;
    }
  }

  public TaggedLineSegment[] getSegments() { return segs; }

  public void addToResult(LineSegment seg)
  {
    resultSegs.add(seg);
  }

  public LineString asLineString()
  {
    return parentLine.getFactory().createLineString(extractCoordinates(resultSegs));
  }

  public LinearRing asLinearRing() {
    return parentLine.getFactory().createLinearRing(extractCoordinates(resultSegs));
  }

  private static Coordinate[] extractCoordinates(List segs)
  {
    Coordinate[] pts = new Coordinate[segs.size() + 1];
    LineSegment seg = null;
    for (int i = 0; i < segs.size(); i++) {
      seg = (LineSegment) segs.get(i);
      pts[i] = seg.p0;
    }
    // add last point
    pts[pts.length - 1] = seg.p1;
    return pts;
  }


}
