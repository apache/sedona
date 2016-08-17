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
package com.vividsolutions.jts.operation.buffer.validate;

import com.vividsolutions.jts.geom.*;

/**
 * Finds the approximate maximum distance from a buffer curve to
 * the originating geometry.
 * This is similar to the Discrete Oriented Hausdorff distance
 * from the buffer curve to the input.
 * <p>
 * The approximate maximum distance is determined by testing
 * all vertices in the buffer curve, as well
 * as midpoints of the curve segments.
 * Due to the way buffer curves are constructed, this
 * should be a very close approximation.
 * 
 * @author mbdavis
 *
 */
public class BufferCurveMaximumDistanceFinder 
{
	private Geometry inputGeom;
  private PointPairDistance maxPtDist = new PointPairDistance();

	public BufferCurveMaximumDistanceFinder(Geometry inputGeom)
	{
		this.inputGeom = inputGeom;
	}
	
	public double findDistance(Geometry bufferCurve)
	{
    computeMaxVertexDistance(bufferCurve);
    computeMaxMidpointDistance(bufferCurve);
    return maxPtDist.getDistance();
	}
	
	public PointPairDistance getDistancePoints()
	{
		return maxPtDist;
	}
	private void computeMaxVertexDistance(Geometry curve)
	{
    MaxPointDistanceFilter distFilter = new MaxPointDistanceFilter(inputGeom);
    curve.apply(distFilter);
    maxPtDist.setMaximum(distFilter.getMaxPointDistance());
	}
	
	private void computeMaxMidpointDistance(Geometry curve)
	{
    MaxMidpointDistanceFilter distFilter = new MaxMidpointDistanceFilter(inputGeom);
    curve.apply(distFilter);
    maxPtDist.setMaximum(distFilter.getMaxPointDistance());
	}
	
  public static class MaxPointDistanceFilter implements CoordinateFilter {
		private PointPairDistance maxPtDist = new PointPairDistance();
		private PointPairDistance minPtDist = new PointPairDistance();
		private Geometry geom;

		public MaxPointDistanceFilter(Geometry geom) {
			this.geom = geom;
		}

		public void filter(Coordinate pt) {
			minPtDist.initialize();
			DistanceToPointFinder.computeDistance(geom, pt, minPtDist);
			maxPtDist.setMaximum(minPtDist);
		}

		public PointPairDistance getMaxPointDistance() {
			return maxPtDist;
		}
	}

  public static class MaxMidpointDistanceFilter 
  	implements CoordinateSequenceFilter 
  	{
		private PointPairDistance maxPtDist = new PointPairDistance();
		private PointPairDistance minPtDist = new PointPairDistance();
		private Geometry geom;

		public MaxMidpointDistanceFilter(Geometry geom) {
			this.geom = geom;
		}

		public void filter(CoordinateSequence seq, int index) 
		{
			if (index == 0)
				return;
			
			Coordinate p0 = seq.getCoordinate(index - 1);
			Coordinate p1 = seq.getCoordinate(index);
			Coordinate midPt = new Coordinate(
					(p0.x + p1.x)/2,
					(p0.y + p1.y)/2);
			
			minPtDist.initialize();
			DistanceToPointFinder.computeDistance(geom, midPt, minPtDist);
			maxPtDist.setMaximum(minPtDist);
		}

		public boolean isGeometryChanged() { return false; }
		
		public boolean isDone() { return false; }
		
		public PointPairDistance getMaxPointDistance() {
			return maxPtDist;
		}
	}

}
