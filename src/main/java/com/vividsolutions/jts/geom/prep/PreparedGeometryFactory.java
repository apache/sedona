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
 * A factory for creating {@link PreparedGeometry}s.
 * It chooses an appropriate implementation of PreparedGeometry
 * based on the geoemtric type of the input geometry.
 * <p>
 * In the future, the factory may accept hints that indicate
 * special optimizations which can be performed.
 * 
 * 
 * @author Martin Davis
 *
 */
public class PreparedGeometryFactory 
{
  /**
   * Creates a new {@link PreparedGeometry} appropriate for the argument {@link Geometry}.
   * 
   * @param geom the geometry to prepare
   * @return the prepared geometry
   */
	public static PreparedGeometry prepare(Geometry geom)
	{
		return (new PreparedGeometryFactory()).create(geom); 
	}

  public PreparedGeometryFactory() {
  }

  /**
   * Creates a new {@link PreparedGeometry} appropriate for the argument {@link Geometry}.
   * 
   * @param geom the geometry to prepare
   * @return the prepared geometry
   */
  public PreparedGeometry create(Geometry geom)
  {
    if (geom instanceof Polygonal) 
      return new PreparedPolygon((Polygonal) geom);
    if (geom instanceof Lineal) 
      return new PreparedLineString((Lineal) geom);
    if (geom instanceof Puntal) 
      return new PreparedPoint((Puntal) geom);
    
    /**
     * Default representation.
     */
    return new BasicPreparedGeometry(geom);
  }
}
