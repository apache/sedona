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

package com.vividsolutions.jts.operation.distance;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryComponentFilter;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.util.LinearComponentExtracter;
import com.vividsolutions.jts.index.strtree.STRtree;

public class FacetSequenceTreeBuilder {
  // 6 seems to be a good facet sequence size
  private static final int FACET_SEQUENCE_SIZE = 6;

  // Seems to be better to use a minimum node capacity
  private static final int STR_TREE_NODE_CAPACITY = 4;

  public static STRtree build(Geometry g) {
    STRtree tree = new STRtree(STR_TREE_NODE_CAPACITY);
    List sections = computeFacetSequences(g);
    for (Iterator i = sections.iterator(); i.hasNext();) {
      FacetSequence section = (FacetSequence) i.next();
      tree.insert(section.getEnvelope(), section);
    }
    tree.build();
    return tree;
  }

  /**
   * Creates facet sequences
   * 
   * @param g
   * @return List<GeometryFacetSequence>
   */
  private static List computeFacetSequences(Geometry g) {
    final List sections = new ArrayList();

    g.apply(new GeometryComponentFilter() {

      public void filter(Geometry geom) {
        CoordinateSequence seq = null;
        if (geom instanceof LineString) {
          seq = ((LineString) geom).getCoordinateSequence();
          addFacetSequences(seq, sections);
        }
        else if (geom instanceof Point) {
          seq = ((Point) geom).getCoordinateSequence();
          addFacetSequences(seq, sections);
        }
      }
    });
    return sections;
  }

  private static void addFacetSequences(CoordinateSequence pts, List sections) {
    int i = 0;
    int size = pts.size();
    while (i <= size - 1) {
      int end = i + FACET_SEQUENCE_SIZE + 1;
      // if only one point remains after this section, include it in this
      // section
      if (end >= size - 1)
        end = size;
      FacetSequence sect = new FacetSequence(pts, i, end);
      sections.add(sect);
      i = i + FACET_SEQUENCE_SIZE;
    }
  }
}
