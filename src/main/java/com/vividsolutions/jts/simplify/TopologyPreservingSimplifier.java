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

package com.vividsolutions.jts.simplify;

import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.util.*;
import com.vividsolutions.jts.util.Debug;

/**
 * Simplifies a geometry and ensures that
 * the result is a valid geometry having the
 * same dimension and number of components as the input,
 * and with the components having the same topological 
 * relationship.
 * <p>
 * If the input is a polygonal geometry
 * ( {@link Polygon} or {@link MultiPolygon} ):
 * <ul>
 * <li>The result has the same number of shells and holes as the input,
 * with the same topological structure
 * <li>The result rings touch at <b>no more</b> than the number of touching points in the input
 * (although they may touch at fewer points).  
 * The key implication of this statement is that if the 
 * input is topologically valid, so is the simplified output. 
 * </ul>
 * For linear geometries, if the input does not contain
 * any intersecting line segments, this property
 * will be preserved in the output.
 * <p>
 * For all geometry types, the result will contain 
 * enough vertices to ensure validity.  For polygons
 * and closed linear geometries, the result will have at
 * least 4 vertices; for open linestrings the result
 * will have at least 2 vertices.
 * <p>
 * All geometry types are handled. 
 * Empty and point geometries are returned unchanged.
 * Empty geometry components are deleted.
 * <p>
 * The simplification uses a maximum-distance difference algorithm
 * similar to the Douglas-Peucker algorithm.
 *
 * <h3>KNOWN BUGS</h3>
 * <ul>
 * <li>May create invalid topology if there are components which are 
 * small relative to the tolerance value.
 * In particular, if a small hole is very near an edge, it is possible for the edge to be moved by
 * a relatively large tolerance value and end up with the hole outside the result shell
 * (or inside another hole).
 * Similarly, it is possible for a small polygon component to end up inside
 * a nearby larger polygon.
 * A workaround is to test for this situation in post-processing and remove
 * any invalid holes or polygons.
 * </ul>
 * 
 * @author Martin Davis
 * @see DouglasPeuckerSimplifier
 *
 */
public class TopologyPreservingSimplifier
{
  public static Geometry simplify(Geometry geom, double distanceTolerance)
  {
    TopologyPreservingSimplifier tss = new TopologyPreservingSimplifier(geom);
    tss.setDistanceTolerance(distanceTolerance);
    return tss.getResultGeometry();
  }

  private Geometry inputGeom;
  private TaggedLinesSimplifier lineSimplifier = new TaggedLinesSimplifier();
  private Map linestringMap;

  public TopologyPreservingSimplifier(Geometry inputGeom)
  {
    this.inputGeom = inputGeom;
 }

  /**
   * Sets the distance tolerance for the simplification.
   * All vertices in the simplified geometry will be within this
   * distance of the original geometry.
   * The tolerance value must be non-negative.  A tolerance value
   * of zero is effectively a no-op.
   *
   * @param distanceTolerance the approximation tolerance to use
   */
  public void setDistanceTolerance(double distanceTolerance) {
    if (distanceTolerance < 0.0)
      throw new IllegalArgumentException("Tolerance must be non-negative");
    lineSimplifier.setDistanceTolerance(distanceTolerance);
  }

  public Geometry getResultGeometry() 
  {
    // empty input produces an empty result
    if (inputGeom.isEmpty()) return (Geometry) inputGeom.clone();
    
    linestringMap = new HashMap();
    inputGeom.apply(new LineStringMapBuilderFilter());
    lineSimplifier.simplify(linestringMap.values());
    Geometry result = (new LineStringTransformer()).transform(inputGeom);
    return result;
  }

  class LineStringTransformer
      extends GeometryTransformer
  {
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent)
    {
      if (coords.size() == 0) return null;
    	// for linear components (including rings), simplify the linestring
      if (parent instanceof LineString) {
        TaggedLineString taggedLine = (TaggedLineString) linestringMap.get(parent);
        return createCoordinateSequence(taggedLine.getResultCoordinates());
      }
      // for anything else (e.g. points) just copy the coordinates
      return super.transformCoordinates(coords, parent);
    }
  }

  /**
   * A filter to add linear geometries to the linestring map 
   * with the appropriate minimum size constraint.
   * Closed {@link LineString}s (including {@link LinearRing}s
   * have a minimum output size constraint of 4, 
   * to ensure the output is valid.
   * For all other linestrings, the minimum size is 2 points.
   * 
   * @author Martin Davis
   *
   */
  class LineStringMapBuilderFilter
      implements GeometryComponentFilter
  {
    /**
     * Filters linear geometries.
     * 
     * geom a geometry of any type 
     */
    public void filter(Geometry geom)
    {
      if (geom instanceof LineString) {
        LineString line = (LineString) geom;
        // skip empty geometries
        if (line.isEmpty()) return;
        
        int minSize = ((LineString) line).isClosed() ? 4 : 2;
        TaggedLineString taggedLine = new TaggedLineString((LineString) line, minSize);
        linestringMap.put(line, taggedLine);
      }
    }
  }

}

