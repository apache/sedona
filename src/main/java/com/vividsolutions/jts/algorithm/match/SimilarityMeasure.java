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

package com.vividsolutions.jts.algorithm.match;

import com.vividsolutions.jts.geom.*;

/**
 * An interface for classes which measures the degree of similarity between two {@link Geometry}s.
 * The computed measure lies in the range [0, 1].
 * Higher measures indicate a great degree of similarity.
 * A measure of 1.0 indicates that the input geometries are identical
 * A measure of 0.0 indicates that the geometries
 * have essentially no similarity.
 * The precise definition of "identical" and "no similarity" may depend on the 
 * exact algorithm being used.
 * 
 * @author mbdavis
 *
 */
public interface SimilarityMeasure
{
	
	double measure(Geometry g1, Geometry g2);
}
