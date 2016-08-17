
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
package com.vividsolutions.jts.geom;

import java.io.Serializable;

/**
 * Creates CoordinateSequences represented as an array of {@link Coordinate}s.
 *
 * @version 1.7
 *
 * @deprecated no longer used
 */
public class DefaultCoordinateSequenceFactory
    implements CoordinateSequenceFactory, Serializable
{
  private static final long serialVersionUID = -4099577099607551657L;
  private static final DefaultCoordinateSequenceFactory instanceObject = new DefaultCoordinateSequenceFactory();

  public DefaultCoordinateSequenceFactory() {
  }

  private Object readResolve() {
  	// see http://www.javaworld.com/javaworld/javatips/jw-javatip122.html
    return DefaultCoordinateSequenceFactory.instance();
  }

  /**
   * Returns the singleton instance of DefaultCoordinateSequenceFactory
   */
  public static DefaultCoordinateSequenceFactory instance() {
    return instanceObject;
  }



  /**
   * Returns a DefaultCoordinateSequence based on the given array (the array is
   * not copied).
   *
   * @param coordinates
   *            the coordinates, which may not be null nor contain null
   *            elements
   */
  public CoordinateSequence create(Coordinate[] coordinates) {
    return new DefaultCoordinateSequence(coordinates);
  }

  /**
   * @see com.vividsolutions.jts.geom.CoordinateSequenceFactory#create(com.vividsolutions.jts.geom.CoordinateSequence)
   */
  public CoordinateSequence create(CoordinateSequence coordSeq) {
    return new DefaultCoordinateSequence(coordSeq);
  }

  /**
   * @see com.vividsolutions.jts.geom.CoordinateSequenceFactory#create(int, int)
   */
  public CoordinateSequence create(int size, int dimension) {
    return new DefaultCoordinateSequence(size);
  }
}