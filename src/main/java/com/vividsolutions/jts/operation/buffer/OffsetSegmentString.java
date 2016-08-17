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
package com.vividsolutions.jts.operation.buffer;

import java.util.*;
import com.vividsolutions.jts.geom.*;

/**
 * A dynamic list of the vertices in a constructed offset curve.
 * Automatically removes adjacent vertices
 * which are closer than a given tolerance.
 * 
 * @author Martin Davis
 *
 */
class OffsetSegmentString 
{
  private static final Coordinate[] COORDINATE_ARRAY_TYPE = new Coordinate[0];

  private ArrayList ptList;
  private PrecisionModel precisionModel = null;
  
  /**
   * The distance below which two adjacent points on the curve 
   * are considered to be coincident.
   * This is chosen to be a small fraction of the offset distance.
   */
  private double minimimVertexDistance = 0.0;

  public OffsetSegmentString()
  {
  	ptList = new ArrayList();
  }
  
  public void setPrecisionModel(PrecisionModel precisionModel)
  {
  	this.precisionModel = precisionModel;
  }
  
  public void setMinimumVertexDistance(double minimimVertexDistance)
  {
  	this.minimimVertexDistance = minimimVertexDistance;
  }
  
  public void addPt(Coordinate pt)
  {
    Coordinate bufPt = new Coordinate(pt);
    precisionModel.makePrecise(bufPt);
    // don't add duplicate (or near-duplicate) points
    if (isRedundant(bufPt))
        return;
    ptList.add(bufPt);
//System.out.println(bufPt);
  }
  
  public void addPts(Coordinate[] pt, boolean isForward)
  {
    if (isForward) {
      for (int i = 0; i < pt.length; i++) {
        addPt(pt[i]);
      }
    }
    else {
      for (int i = pt.length - 1; i >= 0; i--) {
        addPt(pt[i]);
      }     
    }
  }
  
  /**
   * Tests whether the given point is redundant
   * relative to the previous
   * point in the list (up to tolerance).
   * 
   * @param pt
   * @return true if the point is redundant
   */
  private boolean isRedundant(Coordinate pt)
  {
    if (ptList.size() < 1)
    	return false;
    Coordinate lastPt = (Coordinate) ptList.get(ptList.size() - 1);
    double ptDist = pt.distance(lastPt);
    if (ptDist < minimimVertexDistance)
    	return true;
    return false;
  }
  
  public void closeRing()
  {
    if (ptList.size() < 1) return;
    Coordinate startPt = new Coordinate((Coordinate) ptList.get(0));
    Coordinate lastPt = (Coordinate) ptList.get(ptList.size() - 1);
    Coordinate last2Pt = null;
    if (ptList.size() >= 2)
      last2Pt = (Coordinate) ptList.get(ptList.size() - 2);
    if (startPt.equals(lastPt)) return;
    ptList.add(startPt);
  }

  public void reverse()
  {
    
  }
  
  public Coordinate[] getCoordinates()
  {
    /*
     // check that points are a ring - add the startpoint again if they are not
   if (ptList.size() > 1) {
      Coordinate start  = (Coordinate) ptList.get(0);
      Coordinate end    = (Coordinate) ptList.get(ptList.size() - 1);
      if (! start.equals(end) ) addPt(start);
    }
    */
    Coordinate[] coord = (Coordinate[]) ptList.toArray(COORDINATE_ARRAY_TYPE);
    return coord;
  }

  public String toString()
  {
  	GeometryFactory fact = new GeometryFactory();
  	LineString line = fact.createLineString(getCoordinates());
  	return line.toString();
  }
}
