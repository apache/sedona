package com.vividsolutions.jts.edgegraph;

import java.util.Collection;
import java.util.Iterator;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryComponentFilter;
import com.vividsolutions.jts.geom.LineString;

/**
 * Builds an edge graph from geometries containing edges.
 * 
 * @author mdavis
 *
 */
public class EdgeGraphBuilder 
{
  public static EdgeGraph build(Collection geoms) {
    EdgeGraphBuilder builder = new EdgeGraphBuilder();
    builder.add(geoms);
    return builder.getGraph();
  }

  private EdgeGraph graph = new EdgeGraph();

  public EdgeGraphBuilder()
  {
    
  }
  
  public EdgeGraph getGraph()
  {
    return graph;
  }
  
  /**
   * Adds the edges of a Geometry to the graph. 
   * May be called multiple times.
   * Any dimension of Geometry may be added; the constituent edges are
   * extracted.
   * 
   * @param geometry geometry to be added
   */  
  public void add(Geometry geometry) {
    geometry.apply(new GeometryComponentFilter() {
      public void filter(Geometry component) {
        if (component instanceof LineString) {
          add((LineString)component);
        }
      }      
    });
  }
  /**
   * Adds the edges in a collection of {@link Geometry}s to the graph. 
   * May be called multiple times.
   * Any dimension of Geometry may be added.
   * 
   * @param geometries the geometries to be added
   */
  public void add(Collection geometries) 
  {
    for (Iterator i = geometries.iterator(); i.hasNext(); ) {
      Geometry geometry = (Geometry) i.next();
      add(geometry);
    }
  }
  
  private void add(LineString lineString) {
    CoordinateSequence seq = lineString.getCoordinateSequence();
    for (int i = 1; i < seq.size(); i++) {
      graph.addEdge(seq.getCoordinate(i-1), seq.getCoordinate(i));
    }
  }

  
}
