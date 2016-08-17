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
 * Computes the <tt>containsProperly</tt> spatial relationship predicate
 * for {@link PreparedPolygon}s relative to all other {@link Geometry} classes.
 * Uses short-circuit tests and indexing to improve performance. 
 * <p>
 * A Geometry A <tt>containsProperly</tt> another Geometry B iff
 * all points of B are contained in the Interior of A.
 * Equivalently, B is contained in A AND B does not intersect 
 * the Boundary of A.
 * <p>
 * The advantage to using this predicate is that it can be computed
 * efficiently, with no need to compute topology at individual points.
 * In a situation with many geometries intersecting the boundary 
 * of the target geometry, this can make a performance difference.
 * 
 * @author Martin Davis
 */
class PreparedPolygonContainsProperly 
	extends PreparedPolygonPredicate
{
	/**
	 * Computes the </tt>containsProperly</tt> predicate between a {@link PreparedPolygon}
	 * and a {@link Geometry}.
	 * 
	 * @param prep the prepared polygon
	 * @param geom a test geometry
	 * @return true if the polygon properly contains the geometry
	 */
	public static boolean containsProperly(PreparedPolygon prep, Geometry geom)
	{
		PreparedPolygonContainsProperly polyInt = new PreparedPolygonContainsProperly(prep);
    return polyInt.containsProperly(geom);
	}

  /**
   * Creates an instance of this operation.
   * 
   * @param prepPoly the PreparedPolygon to evaluate
   */
	public PreparedPolygonContainsProperly(PreparedPolygon prepPoly)
	{
		super(prepPoly);
	}
	
	/**
	 * Tests whether this PreparedPolygon containsProperly a given geometry.
	 * 
	 * @param geom the test geometry
	 * @return true if the test geometry is contained properly
	 */
	public boolean containsProperly(Geometry geom)
	{
		/**
		 * Do point-in-poly tests first, since they are cheaper and may result
		 * in a quick negative result.
		 * 
		 * If a point of any test components does not lie in the target interior, result is false
		 */
		boolean isAllInPrepGeomAreaInterior = isAllTestComponentsInTargetInterior(geom);
		if (! isAllInPrepGeomAreaInterior) return false;
		
		/**
		 * If any segments intersect, result is false.
		 */
    List lineSegStr = SegmentStringUtil.extractSegmentStrings(geom);
		boolean segsIntersect = prepPoly.getIntersectionFinder().intersects(lineSegStr);
		if (segsIntersect) 
      return false;
		
		/**
		 * Given that no segments intersect, if any vertex of the target
		 * is contained in some test component.
		 * the test is NOT properly contained.
		 */
		if (geom instanceof Polygonal) {
			// TODO: generalize this to handle GeometryCollections
			boolean isTargetGeomInTestArea = isAnyTargetComponentInAreaTest(geom, prepPoly.getRepresentativePoints());
			if (isTargetGeomInTestArea) return false;
		}
		
		return true;
	}
	
}
