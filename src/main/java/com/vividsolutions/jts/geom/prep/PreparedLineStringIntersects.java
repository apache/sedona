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
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.noding.*;
import com.vividsolutions.jts.geom.util.*;

/**
 * Computes the <tt>intersects</tt> spatial relationship predicate
 * for a target {@link PreparedLineString} relative to other {@link Geometry} classes.
 * Uses short-circuit tests and indexing to improve performance. 
 * 
 * @author Martin Davis
 *
 */
class PreparedLineStringIntersects 
{
	/**
	 * Computes the intersects predicate between a {@link PreparedLineString}
	 * and a {@link Geometry}.
	 * 
	 * @param prep the prepared linestring
	 * @param geom a test geometry
	 * @return true if the linestring intersects the geometry
	 */
	public static boolean intersects(PreparedLineString prep, Geometry geom)
	{
		PreparedLineStringIntersects op = new PreparedLineStringIntersects(prep);
    return op.intersects(geom);
	}

	protected PreparedLineString prepLine;

  /**
   * Creates an instance of this operation.
   * 
   * @param prepPoly the target PreparedLineString
   */
	public PreparedLineStringIntersects(PreparedLineString prepLine)
	{
		this.prepLine = prepLine;
	}
	
	/**
	 * Tests whether this geometry intersects a given geometry.
	 * 
	 * @param geom the test geometry
	 * @return true if the test geometry intersects
	 */
	public boolean intersects(Geometry geom)
	{
		/**
		 * If any segments intersect, obviously intersects = true
		 */
    List lineSegStr = SegmentStringUtil.extractSegmentStrings(geom);
    // only request intersection finder if there are segments (ie NOT for point inputs)
    if (lineSegStr.size() > 0) {
  		boolean segsIntersect = prepLine.getIntersectionFinder().intersects(lineSegStr);
  		// MD - performance testing
  //		boolean segsIntersect = false;
  		if (segsIntersect) 
        return true;
    }
		/**
		 * For L/L case we are done
		 */
		if (geom.getDimension() == 1) return false;
		
		/**
		 * For L/A case, need to check for proper inclusion of the target in the test
		 */
		if (geom.getDimension() == 2
				&& prepLine.isAnyTargetComponentInTest(geom)) return true;
		
		/** 
		 * For L/P case, need to check if any points lie on line(s)
		 */
		if (geom.getDimension() == 0)
			return isAnyTestPointInTarget(geom);
		
		return false;
	}
	  
  /**
   * Tests whether any representative point of the test Geometry intersects
   * the target geometry.
   * Only handles test geometries which are Puntal (dimension 0)
   * 
   * @param geom a Puntal geometry to test
   * @return true if any point of the argument intersects the prepared geometry
   */
	protected boolean isAnyTestPointInTarget(Geometry testGeom)
	{
		/**
		 * This could be optimized by using the segment index on the lineal target.
		 * However, it seems like the L/P case would be pretty rare in practice.
		 */
		PointLocator locator = new PointLocator();
    List coords = ComponentCoordinateExtracter.getCoordinates(testGeom);
    for (Iterator i = coords.iterator(); i.hasNext(); ) {
      Coordinate p = (Coordinate) i.next();
      if (locator.intersects(p, prepLine.getGeometry()))
        return true;
    }
		return false;
	}

}
