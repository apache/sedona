
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
package com.vividsolutions.jts.operation.polygonize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryComponentFilter;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Polygonizes a set of {@link Geometry}s which contain linework that
 * represents the edges of a planar graph.
 * All types of Geometry are accepted as input;  
 * the constituent linework is extracted as the edges to be polygonized.
 * The processed edges must be correctly noded; that is, they must only meet
 * at their endpoints.  Polygonization will accept incorrectly noded input
 * but will not form polygons from non-noded edges, 
 * and reports them as errors.
 * <p>
 * The Polygonizer reports the follow kinds of errors:
 * <ul>
 * <li><b>Dangles</b> - edges which have one or both ends which are not incident on another edge endpoint
 * <li><b>Cut Edges</b> - edges which are connected at both ends but which do not form part of polygon
 * <li><b>Invalid Ring Lines</b> - edges which form rings which are invalid
 * (e.g. the component lines contain a self-intersection)
 * </ul>
 * Polygonization supports extracting only polygons which form a valid polygonal geometry.
 * The set of extracted polygons is guaranteed to be edge-disjoint.
 * This is useful for situations where it is known that the input lines form a
 * valid polygonal geometry.
 *
 * @version 1.7
 */
public class Polygonizer
{  
  /**
   * Adds every linear element in a {@link Geometry} into the polygonizer graph.
   */
  private class LineStringAdder
      implements GeometryComponentFilter
  {
    public void filter(Geometry g) {
      if (g instanceof LineString)
        add((LineString) g);
    }
  }

  // default factory
  private LineStringAdder lineStringAdder = new LineStringAdder();

  protected PolygonizeGraph graph;
  // initialize with empty collections, in case nothing is computed
  protected Collection dangles = new ArrayList();
  protected List cutEdges = new ArrayList();
  protected List invalidRingLines = new ArrayList();

  protected List holeList = null;
  protected List shellList = null;
  protected List polyList = null;

  private boolean isCheckingRingsValid = true;
  private boolean extractOnlyPolygonal;

  private GeometryFactory geomFactory = null;

  /**
   * Creates a polygonizer with the same {@link GeometryFactory}
   * as the input {@link Geometry}s.
   * The output mask is {@link #ALL_POLYS}.
   */
  public Polygonizer()
  {
    this(false);
  }
  
  /**
   * Creates a polygonizer and allow specifyng if only polygons which form a valid polygonal geometry are to be extracted.
   * 
   * @param extractOnlyPolygonal true if only polygons which form a valid polygonal geometry are to be extracted
   */
  public Polygonizer(boolean extractOnlyPolygonal)
  {
    this.extractOnlyPolygonal = extractOnlyPolygonal;
  }

  /**
   * Adds a collection of geometries to the edges to be polygonized.
   * May be called multiple times.
   * Any dimension of Geometry may be added;
   * the constituent linework will be extracted and used.
   *
   * @param geomList a list of {@link Geometry}s with linework to be polygonized
   */
  public void add(Collection geomList)
  {
    for (Iterator i = geomList.iterator(); i.hasNext(); ) {
      Geometry geometry = (Geometry) i.next();
      add(geometry);
    }
  }

  /**
   * Add a {@link Geometry} to the edges to be polygonized.
   * May be called multiple times.
   * Any dimension of Geometry may be added;
   * the constituent linework will be extracted and used
   *
   * @param g a {@link Geometry} with linework to be polygonized
   */
  public void add(Geometry g)
  {
    g.apply(lineStringAdder);
  }

  /**
   * Adds a linestring to the graph of polygon edges.
   *
   * @param line the {@link LineString} to add
   */
  private void add(LineString line)
  {
    // record the geometry factory for later use
    geomFactory  = line.getFactory();
    // create a new graph using the factory from the input Geometry
    if (graph == null)
      graph = new PolygonizeGraph(geomFactory);
    graph.addEdge(line);
  }

  /**
   * Allows disabling the valid ring checking, 
   * to optimize situations where invalid rings are not expected.
   * <p>
   * The default is <code>true</code.
   * 
   * @param isCheckingRingsValid true if generated rings should be checked for validity
   */
  public void setCheckRingsValid(boolean isCheckingRingsValid)
  {
    this.isCheckingRingsValid = isCheckingRingsValid;
  }
  
  /**
   * Gets the list of polygons formed by the polygonization.
   * @return a collection of {@link Polygon}s
   */
  public Collection getPolygons()
  {
    polygonize();
    return polyList;
  }

  /**
   * Gets a geometry representing the polygons formed by the polygonization.
   * If a valid polygonal geometry was extracted the result is a {@linkl Polygonal} geometry. 
   * 
   * @return a geometry containing the polygons
   */
  public Geometry getGeometry()
  {
    if (geomFactory == null) geomFactory = new GeometryFactory();
    polygonize();
    if (extractOnlyPolygonal) {
      return geomFactory.buildGeometry(polyList);
    }
    // result may not be valid Polygonal, so return as a GeometryCollection
    return geomFactory.createGeometryCollection(GeometryFactory.toGeometryArray(polyList));
  }

  /**
   * Gets the list of dangling lines found during polygonization.
   * @return a collection of the input {@link LineString}s which are dangles
   */
  public Collection getDangles()
  {
    polygonize();
    return dangles;
  }

  /**
   * Gets the list of cut edges found during polygonization.
   * @return a collection of the input {@link LineString}s which are cut edges
   */
  public Collection getCutEdges()
  {
    polygonize();
    return cutEdges;
  }

  /**
   * Gets the list of lines forming invalid rings found during polygonization.
   * @return a collection of the input {@link LineString}s which form invalid rings
   */
  public Collection getInvalidRingLines()
  {
    polygonize();
    return invalidRingLines;
  }

  /**
   * Performs the polygonization, if it has not already been carried out.
   */
  private void polygonize()
  {
    // check if already computed
    if (polyList != null) return;
    polyList = new ArrayList();

    // if no geometries were supplied it's possible that graph is null
    if (graph == null) return;

    dangles = graph.deleteDangles();
    cutEdges = graph.deleteCutEdges();
    List edgeRingList = graph.getEdgeRings();

    //Debug.printTime("Build Edge Rings");

    List validEdgeRingList = new ArrayList();
    invalidRingLines = new ArrayList();
    if (isCheckingRingsValid) {
      findValidRings(edgeRingList, validEdgeRingList, invalidRingLines);
    }
    else {
      validEdgeRingList = edgeRingList;
    }
    //Debug.printTime("Validate Rings");
    
    findShellsAndHoles(validEdgeRingList);
    assignHolesToShells(holeList, shellList);
    // order the shells to make any subsequent processing deterministic
    Collections.sort(shellList, new EdgeRing.EnvelopeComparator());

    //Debug.printTime("Assign Holes");
    
    boolean includeAll = true;
    if (extractOnlyPolygonal) {
      findDisjointShells(shellList);
      includeAll = false;
    }
    polyList = extractPolygons(shellList, includeAll);
  }

  private void findValidRings(List edgeRingList, List validEdgeRingList, List invalidRingList)
  {
    for (Iterator i = edgeRingList.iterator(); i.hasNext(); ) {
      EdgeRing er = (EdgeRing) i.next();
      if (er.isValid())
        validEdgeRingList.add(er);
      else
        invalidRingList.add(er.getLineString());
    }
  }

  private void findShellsAndHoles(List edgeRingList)
  {
    holeList = new ArrayList();
    shellList = new ArrayList();
    for (Iterator i = edgeRingList.iterator(); i.hasNext(); ) {
      EdgeRing er = (EdgeRing) i.next();
      er.computeHole();
      if (er.isHole())
        holeList.add(er);
      else
        shellList.add(er);
    }
  }

  private static void assignHolesToShells(List holeList, List shellList)
  {
    for (Iterator i = holeList.iterator(); i.hasNext(); ) {
      EdgeRing holeER = (EdgeRing) i.next();
      assignHoleToShell(holeER, shellList);
      /*
      if ( ! holeER.hasShell()) {
        System.out.println("DEBUG: Outer hole: " + holeER);
      }
      */
    }
  }

  private static void assignHoleToShell(EdgeRing holeER, List shellList)
  {
    EdgeRing shell = EdgeRing.findEdgeRingContaining(holeER, shellList);
    if (shell != null) {
      shell.addHole(holeER);
    }
  }

  private static void findDisjointShells(List shellList) {
    findOuterShells(shellList);
    
    boolean isMoreToScan;
    do {
      isMoreToScan = false;
      for (Iterator i = shellList.iterator(); i.hasNext(); ) {
        EdgeRing er = (EdgeRing) i.next();
        if (er.isIncludedSet()) 
          continue;
        er.updateIncluded();
        if (! er.isIncludedSet()) {
          isMoreToScan = true;
        }
      }
    } while (isMoreToScan);
  }

  /**
   * For each outer hole finds and includes a single outer shell.
   * This seeds the travesal algorithm for finding only polygonal shells.
   *  
   * @param shellList the list of shell EdgeRings
   */
  private static void findOuterShells(List shellList) {

    for (Iterator i = shellList.iterator(); i.hasNext();) {
      EdgeRing er = (EdgeRing) i.next();
      EdgeRing outerHoleER = er.getOuterHole();
      if (outerHoleER != null && ! outerHoleER.isProcessed()) {
        er.setIncluded(true);
        outerHoleER.setProcessed(true);
      }
    }
  }
  
  private static List extractPolygons(List shellList, boolean includeAll) {
    List polyList = new ArrayList();
    for (Iterator i = shellList.iterator(); i.hasNext();) {
      EdgeRing er = (EdgeRing) i.next();
      if (includeAll || er.isIncluded()) {
        polyList.add(er.getPolygon());
      }
    }
    return polyList;
  }
}
