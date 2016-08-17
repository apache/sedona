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

package com.vividsolutions.jts.precision;

import com.vividsolutions.jts.geom.*;

/**
 * Reduces the precision of the {@link Coordinate}s in a
 * {@link CoordinateSequence} to match the supplied {@link PrecisionModel}.
 * Uses {@link PrecisionModel#makePrecise(double)}.
 * The input is modified in-place, so
 * it should be cloned beforehand if the
 * original should not be modified.
 * 
 * @author mbdavis
 *
 */
public class CoordinatePrecisionReducerFilter
	implements CoordinateSequenceFilter
{
	private PrecisionModel precModel;
	
	/**
	 * Creates a new precision reducer filter.
	 * 
	 * @param precModel the PrecisionModel to use 
	 */
	public CoordinatePrecisionReducerFilter(PrecisionModel precModel)
	{
		this.precModel = precModel;
	}
	
	/**
	 * Rounds the Coordinates in the sequence to match the PrecisionModel
	 */
	public void filter(CoordinateSequence seq, int i)
	{
		seq.setOrdinate(i, 0, precModel.makePrecise(seq.getOrdinate(i, 0)));
		seq.setOrdinate(i, 1, precModel.makePrecise(seq.getOrdinate(i, 1)));
	}
  
	/**
	 * Always runs over all geometry components.
	 *  
	 * @return false
	 */
  public boolean isDone()  {  	return false;  }
  
  /**
   * Always reports that the geometry has changed
   * 
   * @return true
   */
  public boolean isGeometryChanged() { return true;   }
}
