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

package com.vividsolutions.jts.shape.fractal;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.math.Vector2D;
import com.vividsolutions.jts.shape.*;

public class KochSnowflakeBuilder 
extends GeometricShapeBuilder
{
	private CoordinateList coordList = new CoordinateList();
	
	public KochSnowflakeBuilder(GeometryFactory geomFactory)
	{
		super(geomFactory);
	}
	
	public static int recursionLevelForSize(int numPts)
	{
		double pow4 = numPts / 3;
		double exp = Math.log(pow4)/Math.log(4);
		return (int) exp;
	}
	
	public Geometry getGeometry()
	{
		int level = recursionLevelForSize(numPts);
		LineSegment baseLine = getSquareBaseLine();
		Coordinate[] pts = getBoundary(level, baseLine.getCoordinate(0), baseLine.getLength());
		return geomFactory.createPolygon(
				geomFactory.createLinearRing(pts), null);
	}
	
	/**
	 * The height of an equilateral triangle of side one
	 */
	private static final double HEIGHT_FACTOR = Math.sin(Math.PI / 3.0);
	private static final double ONE_THIRD = 1.0/3.0;
	private static final double THIRD_HEIGHT = HEIGHT_FACTOR/3.0;
	private static final double TWO_THIRDS = 2.0/3.0;
	
	private Coordinate[] getBoundary(int level, Coordinate origin, double width) 
	{
		double y = origin.y;
		// for all levels beyond 0 need to vertically shift shape by height of one "arm" to centre it
		if (level > 0) {
			y += THIRD_HEIGHT * width;
		}
		
		Coordinate p0 = new Coordinate(origin.x, y);
		Coordinate p1 = new Coordinate(origin.x + width/2, y + width * HEIGHT_FACTOR);
		Coordinate p2 = new Coordinate(origin.x + width, y);
		addSide(level, p0, p1);
		addSide(level, p1, p2);
		addSide(level, p2, p0);
		coordList.closeRing();
		return coordList.toCoordinateArray();
	}

	public void addSide(int level, Coordinate p0, Coordinate p1) {
		if (level == 0)
			addSegment(p0, p1);
		else {
			Vector2D base = Vector2D.create(p0, p1);
			Coordinate midPt = base.multiply(0.5).translate(p0);
			
			Vector2D heightVec = base.multiply(THIRD_HEIGHT);
			Vector2D offsetVec = heightVec.rotateByQuarterCircle(1);
			Coordinate offsetPt = offsetVec.translate(midPt);
			
			int n2 = level - 1;
			Coordinate thirdPt = base.multiply(ONE_THIRD).translate(p0);
			Coordinate twoThirdPt = base.multiply(TWO_THIRDS).translate(p0);
			
			// construct sides recursively
			addSide(n2, p0, thirdPt);
			addSide(n2, thirdPt, offsetPt);
			addSide(n2, offsetPt, twoThirdPt);
			addSide(n2, twoThirdPt, p1);
		}
	}
		
	private void addSegment(Coordinate p0, Coordinate p1)
	{
		coordList.add(p1);
	}
	
}
