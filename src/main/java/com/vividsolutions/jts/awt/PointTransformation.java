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
package com.vividsolutions.jts.awt;

import java.awt.geom.Point2D;
import com.vividsolutions.jts.geom.*;

/**
 * Transforms a geometry {@link Coordinate} into a Java2D {@link Point},
 * possibly with a mathematical transformation of the ordinate values.
 * Transformation from a model coordinate system to a view coordinate system 
 * can be efficiently performed by supplying an appropriate transformation.
 * 
 * @author Martin Davis
 */
public interface PointTransformation {
	/**
	 * Transforms a {@link Coordinate} into a Java2D {@link Point}.
	 * 
	 * @param src the source Coordinate 
	 * @param dest the destination Point
	 */
  public void transform(Coordinate src, Point2D dest);
}