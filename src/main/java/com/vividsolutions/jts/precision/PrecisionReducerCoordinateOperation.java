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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.util.GeometryEditor;

public class PrecisionReducerCoordinateOperation extends
		GeometryEditor.CoordinateOperation 
{
  private PrecisionModel targetPM;
  private boolean removeCollapsed = true;

	public PrecisionReducerCoordinateOperation(PrecisionModel targetPM, boolean removeCollapsed)
	{
		this.targetPM = targetPM;
		this.removeCollapsed = removeCollapsed;
	}
	
	public Coordinate[] edit(Coordinate[] coordinates, Geometry geom) {
		if (coordinates.length == 0)
			return null;

		Coordinate[] reducedCoords = new Coordinate[coordinates.length];
		// copy coordinates and reduce
		for (int i = 0; i < coordinates.length; i++) {
			Coordinate coord = new Coordinate(coordinates[i]);
			targetPM.makePrecise(coord);
			reducedCoords[i] = coord;
		}
		// remove repeated points, to simplify returned geometry as much as possible
		CoordinateList noRepeatedCoordList = new CoordinateList(reducedCoords,
				false);
		Coordinate[] noRepeatedCoords = noRepeatedCoordList.toCoordinateArray();

		/**
		 * Check to see if the removal of repeated points collapsed the coordinate
		 * List to an invalid length for the type of the parent geometry. It is not
		 * necessary to check for Point collapses, since the coordinate list can
		 * never collapse to less than one point. If the length is invalid, return
		 * the full-length coordinate array first computed, or null if collapses are
		 * being removed. (This may create an invalid geometry - the client must
		 * handle this.)
		 */
		int minLength = 0;
		if (geom instanceof LineString)
			minLength = 2;
		if (geom instanceof LinearRing)
			minLength = 4;

		Coordinate[] collapsedCoords = reducedCoords;
		if (removeCollapsed)
			collapsedCoords = null;

		// return null or orginal length coordinate array
		if (noRepeatedCoords.length < minLength) {
			return collapsedCoords;
		}

		// ok to return shorter coordinate array
		return noRepeatedCoords;
	}
}
