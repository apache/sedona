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
package com.vividsolutions.jts.geom.util;

import java.util.*;
import com.vividsolutions.jts.geom.*;

/**
 * Methods to map various collections 
 * of {@link Geometry}s  
 * via defined mapping functions.
 * 
 * @author Martin Davis
 *
 */
public class GeometryMapper 
{
  /**
   * Maps the members of a {@link Geometry}
   * (which may be atomic or composite)
   * into another <tt>Geometry</tt> of most specific type.
   * <tt>null</tt> results are skipped.
   * In the case of hierarchical {@link GeometryCollection}s,
   * only the first level of members are mapped.
   *  
   * @param geom the input atomic or composite geometry
   * @param op the mapping operation
   * @return a result collection or geometry of most specific type
   */
  public static Geometry map(Geometry geom, MapOp op)
  {
    List mapped = new ArrayList();
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      Geometry g = op.map(geom.getGeometryN(i));
      if (g != null)
        mapped.add(g);
    }
    return geom.getFactory().buildGeometry(mapped);
  }
  
  public static Collection map(Collection geoms, MapOp op)
  {
    List mapped = new ArrayList();
    for (Iterator i = geoms.iterator(); i.hasNext(); ) {
      Geometry g = (Geometry) i.next();
      Geometry gr = op.map(g);
      if (gr != null)
        mapped.add(gr);
    }
    return mapped;
  }
  
  /**
   * An interface for geometry functions used for mapping.
   * 
   * @author Martin Davis
   *
   */
  public interface MapOp 
  {
    /**
     * Computes a new geometry value.
     * 
     * @param g the input geometry
     * @return a result geometry
     */
    Geometry map(Geometry g);
  }
}
