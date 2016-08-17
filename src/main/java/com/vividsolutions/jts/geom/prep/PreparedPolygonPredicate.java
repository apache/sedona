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

package com.vividsolutions.jts.geom.prep;

import java.util.*;

import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.algorithm.locate.PointOnGeometryLocator;
import com.vividsolutions.jts.algorithm.locate.SimplePointInAreaLocator;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.util.*;

/**
 * A base class for predicate operations on {@link PreparedPolygon}s.
 * 
 * @author mbdavis
 *
 */
abstract class PreparedPolygonPredicate 
{
	protected PreparedPolygon prepPoly;
  private PointOnGeometryLocator targetPointLocator;

  /**
   * Creates an instance of this operation.
   * 
   * @param prepPoly the PreparedPolygon to evaluate
   */
	public PreparedPolygonPredicate(PreparedPolygon prepPoly)
	{
		this.prepPoly = prepPoly;
    targetPointLocator = prepPoly.getPointLocator();
	}
	
  /**
   * Tests whether all components of the test Geometry 
	 * are contained in the target geometry.
   * Handles both linear and point components.
   * 
   * @param geom a geometry to test
   * @return true if all componenta of the argument are contained in the target geometry
   */
	protected boolean isAllTestComponentsInTarget(Geometry testGeom)
	{
    List coords = ComponentCoordinateExtracter.getCoordinates(testGeom);
    for (Iterator i = coords.iterator(); i.hasNext(); ) {
      Coordinate p = (Coordinate) i.next();
      int loc = targetPointLocator.locate(p);
      if (loc == Location.EXTERIOR)
        return false;
    }
		return true;
	}
	
  /**
   * Tests whether all components of the test Geometry 
	 * are contained in the interior of the target geometry.
   * Handles both linear and point components.
   * 
   * @param geom a geometry to test
   * @return true if all componenta of the argument are contained in the target geometry interior
   */
	protected boolean isAllTestComponentsInTargetInterior(Geometry testGeom)
	{
    List coords = ComponentCoordinateExtracter.getCoordinates(testGeom);
    for (Iterator i = coords.iterator(); i.hasNext(); ) {
      Coordinate p = (Coordinate) i.next();
      int loc = targetPointLocator.locate(p);
      if (loc != Location.INTERIOR)
        return false;
    }
		return true;
	}
	
  /**
   * Tests whether any component of the test Geometry intersects
   * the area of the target geometry.
   * Handles test geometries with both linear and point components.
   * 
   * @param geom a geometry to test
   * @return true if any component of the argument intersects the prepared area geometry
   */
	protected boolean isAnyTestComponentInTarget(Geometry testGeom)
	{
    List coords = ComponentCoordinateExtracter.getCoordinates(testGeom);
    for (Iterator i = coords.iterator(); i.hasNext(); ) {
      Coordinate p = (Coordinate) i.next();
      int loc = targetPointLocator.locate(p);
      if (loc != Location.EXTERIOR)
        return true;
    }
		return false;
	}

  /**
   * Tests whether any component of the test Geometry intersects
   * the interior of the target geometry.
   * Handles test geometries with both linear and point components.
   * 
   * @param geom a geometry to test
   * @return true if any component of the argument intersects the prepared area geometry interior
   */
	protected boolean isAnyTestComponentInTargetInterior(Geometry testGeom)
	{
    List coords = ComponentCoordinateExtracter.getCoordinates(testGeom);
    for (Iterator i = coords.iterator(); i.hasNext(); ) {
      Coordinate p = (Coordinate) i.next();
      int loc = targetPointLocator.locate(p);
      if (loc == Location.INTERIOR)
        return true;
    }
		return false;
	}


	/**
	 * Tests whether any component of the target geometry 
	 * intersects the test geometry (which must be an areal geometry) 
	 * 
	 * @param geom the test geometry
	 * @param repPts the representative points of the target geometry
	 * @return true if any component intersects the areal test geometry
	 */
	protected boolean isAnyTargetComponentInAreaTest(Geometry testGeom, List targetRepPts)
	{
		PointOnGeometryLocator piaLoc = new SimplePointInAreaLocator(testGeom);
    for (Iterator i = targetRepPts.iterator(); i.hasNext(); ) {
      Coordinate p = (Coordinate) i.next();
      int loc = piaLoc.locate(p);
      if (loc != Location.EXTERIOR)
        return true;
    }
		return false;
	}

}
