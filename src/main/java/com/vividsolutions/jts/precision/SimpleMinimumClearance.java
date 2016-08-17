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

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;

/**
 * Computes the minimum clearance of a geometry or 
 * set of geometries.
 * <p>
 * The <b>Minimum Clearance</b> is a measure of
 * what magnitude of perturbation of its vertices can be tolerated
 * by a geometry before it becomes topologically invalid.
 * <p>
 * This class uses an inefficient O(N^2) scan.  
 * It is primarily for testing purposes.
 * 
 * 
 * @see MinimumClearance
 * @author Martin Davis
 *
 */
public class SimpleMinimumClearance 
{
  public static double getDistance(Geometry g)
  {
    SimpleMinimumClearance rp = new SimpleMinimumClearance(g);
    return rp.getDistance();
  }
  
  public static Geometry getLine(Geometry g)
  {
    SimpleMinimumClearance rp = new SimpleMinimumClearance(g);
    return rp.getLine();
  }
  
  private Geometry inputGeom;
  private double minClearance;
  private Coordinate[] minClearancePts;
  
  public SimpleMinimumClearance(Geometry geom)
  {
    inputGeom = geom;
  }
  
  public double getDistance()
  {
    compute();
    return minClearance;
  }
  
  public LineString getLine()
  {
    compute();
    return inputGeom.getFactory().createLineString(minClearancePts);
  }
  
  private void compute()
  {
    if (minClearancePts != null) return;
    minClearancePts = new Coordinate[2];
    minClearance = Double.MAX_VALUE;
    inputGeom.apply(new VertexCoordinateFilter());
  }
  
  private void updateClearance(double candidateValue, Coordinate p0, Coordinate p1)
  {
    if (candidateValue < minClearance) {
      minClearance = candidateValue;
      minClearancePts[0] = new Coordinate(p0);
      minClearancePts[1] = new Coordinate(p1);
    }
  }
  
  private void updateClearance(double candidateValue, Coordinate p, 
      Coordinate seg0, Coordinate seg1)
  {
    if (candidateValue < minClearance) {
      minClearance = candidateValue;
      minClearancePts[0] = new Coordinate(p);
      LineSegment seg = new LineSegment(seg0, seg1);
      minClearancePts[1] = new Coordinate(seg.closestPoint(p));
    }
  }
  
  private class VertexCoordinateFilter 
  implements CoordinateFilter
  {
    public VertexCoordinateFilter()
    {
      
    }
    
    public void filter(Coordinate coord) {
      inputGeom.apply(new ComputeMCCoordinateSequenceFilter(coord));
    }
  }
  
  private class ComputeMCCoordinateSequenceFilter 
  implements CoordinateSequenceFilter 
  {
    private Coordinate queryPt;
    
    public ComputeMCCoordinateSequenceFilter(Coordinate queryPt)
    {
      this.queryPt = queryPt;
    }
    public void filter(CoordinateSequence seq, int i) {
      // compare to vertex
      checkVertexDistance(seq.getCoordinate(i));
      
      // compare to segment, if this is one
      if (i > 0) {
        checkSegmentDistance(seq.getCoordinate(i - 1), seq.getCoordinate(i));
      }
    }
    
    private void checkVertexDistance(Coordinate vertex)
    {
      double vertexDist = vertex.distance(queryPt);
      if (vertexDist > 0) {
        updateClearance(vertexDist, queryPt, vertex);
      }
    }
    
    private void checkSegmentDistance(Coordinate seg0, Coordinate seg1)
    {
        if (queryPt.equals2D(seg0) || queryPt.equals2D(seg1))
          return;
        double segDist = CGAlgorithms.distancePointLine(queryPt, seg1, seg0);
        if (segDist > 0) 
          updateClearance(segDist, queryPt, seg1, seg0);
    }
    
    public boolean isDone() {
      return false;
    }
    
    public boolean isGeometryChanged() {
      return false;
    }
    
  }
}
