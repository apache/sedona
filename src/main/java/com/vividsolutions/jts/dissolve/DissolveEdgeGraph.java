package com.vividsolutions.jts.dissolve;

import com.vividsolutions.jts.edgegraph.EdgeGraph;
import com.vividsolutions.jts.edgegraph.HalfEdge;
import com.vividsolutions.jts.geom.Coordinate;


/**
 * A graph containing {@link DissolveHalfEdge}s.
 * 
 * @author Martin Davis
 *
 */
class DissolveEdgeGraph extends EdgeGraph
{
  protected HalfEdge createEdge(Coordinate p0)
  {
    return new DissolveHalfEdge(p0);
  }
  

}
