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

import com.vividsolutions.jts.geom.*;

/**
 * An interface for classes which prepare {@link Geometry}s 
 * in order to optimize the performance 
 * of repeated calls to specific geometric operations.
 * <p>
 * A given implementation may provide optimized implementations
 * for only some of the specified methods, 
 * and delegate the remaining methods to the original {@link Geometry} operations.
 * An implementation may also only optimize certain situations,
 * and delegate others. 
 * See the implementing classes for documentation about which methods and situations
 * they optimize.
 * <p>
 * Subclasses are intended to be thread-safe, to allow <code>PreparedGeometry</code>
 * to be used in a multi-threaded context 
 * (which allows extracting maximum benefit from the prepared state).
 * 
 * @author Martin Davis
 *
 */
public interface PreparedGeometry 
{
	
	/**
	 * Gets the original {@link Geometry} which has been prepared.
	 * 
	 * @return the base geometry
	 */
	Geometry getGeometry();

	/**
	 * Tests whether the base {@link Geometry} contains a given geometry.
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry contains the given Geometry
	 * 
	 * @see Geometry#contains(Geometry)
	 */
	boolean contains(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} properly contains a given geometry.
	 * <p>
	 * The <code>containsProperly</code> predicate has the following equivalent definitions:
	 * <ul>
	 * <li>Every point of the other geometry is a point of this geometry's interior.
	 * <li>The DE-9IM Intersection Matrix for the two geometries matches 
	 * <code>[T**FF*FF*]</code>
	 * </ul>
	 * In other words, if the test geometry has any interaction with the boundary of the target
	 * geometry the result of <tt>containsProperly</tt> is <tt>false</tt>. 
	 * This is different semantics to the {@link Geometry#contains} predicate,
	 * in which test geometries can intersect the target's boundary and still be contained.
	 * <p>
	 * The advantage of using this predicate is that it can be computed
	 * efficiently, since it avoids the need to compute the full topological relationship
	 * of the input boundaries in cases where they intersect.
	 * <p>
	 * An example use case is computing the intersections
	 * of a set of geometries with a large polygonal geometry.  
	 * Since <tt>intersection</tt> is a fairly slow operation, it can be more efficient
	 * to use <tt>containsProperly</tt> to filter out test geometries which lie
	 * wholly inside the area.  In these cases the intersection is
	 * known <i>a priori</i> to be exactly the original test geometry. 
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry properly contains the given Geometry
	 * 
	 * @see Geometry#contains
	 * 
	 */
	boolean containsProperly(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} is covered by a given geometry.
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry is covered by the given Geometry
	 * 
	 * @see Geometry#coveredBy(Geometry)
	 */
	boolean coveredBy(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} covers a given geometry.
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry covers the given Geometry
	 * 
	 * @see Geometry#covers(Geometry)
	 */
	boolean covers(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} crosses a given geometry.
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry crosses the given Geometry
	 * 
	 * @see Geometry#crosses(Geometry)
	 */
	boolean crosses(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} is disjoint from a given geometry.
	 * This method supports {@link GeometryCollection}s as input
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry is disjoint from the given Geometry
	 * 
	 * @see Geometry#disjoint(Geometry)
	 */
	boolean disjoint(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} intersects a given geometry.
	 * This method supports {@link GeometryCollection}s as input
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry intersects the given Geometry
	 * 
	 * @see Geometry#intersects(Geometry)
	 */
	boolean intersects(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} overlaps a given geometry.
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry overlaps the given Geometry
	 * 
	 * @see Geometry#overlaps(Geometry)
	 */
	boolean overlaps(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} touches a given geometry.
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry touches the given Geometry
	 * 
	 * @see Geometry#touches(Geometry)
	 */
	boolean touches(Geometry geom);

	/**
	 * Tests whether the base {@link Geometry} is within a given geometry.
	 * 
	 * @param geom the Geometry to test
	 * @return true if this Geometry is within the given Geometry
	 * 
	 * @see Geometry#within(Geometry)
	 */
	boolean within(Geometry geom);

}
