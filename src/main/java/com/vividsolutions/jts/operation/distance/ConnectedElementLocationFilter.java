
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
package com.vividsolutions.jts.operation.distance;

import java.util.*;
import com.vividsolutions.jts.geom.*;

/**
 * A ConnectedElementPointFilter extracts a single point
 * from each connected element in a Geometry
 * (e.g. a polygon, linestring or point)
 * and returns them in a list. The elements of the list are 
 * {@link com.vividsolutions.jts.operation.distance.GeometryLocation}s.
 *
 * @version 1.7
 */
public class ConnectedElementLocationFilter
  implements GeometryFilter
{

  /**
   * Returns a list containing a point from each Polygon, LineString, and Point
   * found inside the specified geometry. Thus, if the specified geometry is
   * not a GeometryCollection, an empty list will be returned. The elements of the list 
   * are {@link com.vividsolutions.jts.operation.distance.GeometryLocation}s.
   */  
  public static List getLocations(Geometry geom)
  {
    List locations = new ArrayList();
    geom.apply(new ConnectedElementLocationFilter(locations));
    return locations;
  }

  private List locations;

  ConnectedElementLocationFilter(List locations)
  {
    this.locations = locations;
  }

  public void filter(Geometry geom)
  {
    if (geom instanceof Point
      || geom instanceof LineString
      || geom instanceof Polygon )
      locations.add(new GeometryLocation(geom, 0, geom.getCoordinate()));
  }

}
