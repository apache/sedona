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

package com.vividsolutions.jts.noding.snapround;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.ItemVisitor;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.chain.MonotoneChain;
import com.vividsolutions.jts.index.chain.MonotoneChainSelectAction;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.noding.NodedSegmentString;
import com.vividsolutions.jts.noding.SegmentString;

/**
 * "Snaps" all {@link SegmentString}s in a {@link SpatialIndex} containing
 * {@link MonotoneChain}s to a given {@link HotPixel}.
 *
 * @version 1.7
 */
public class MCIndexPointSnapper
{
  //public static final int nSnaps = 0;

  private STRtree index;

  public MCIndexPointSnapper(SpatialIndex index) {
    this.index = (STRtree) index;
  }

  /**
   * Snaps (nodes) all interacting segments to this hot pixel.
   * The hot pixel may represent a vertex of an edge,
   * in which case this routine uses the optimization
   * of not noding the vertex itself
   *
   * @param hotPixel the hot pixel to snap to
   * @param parentEdge the edge containing the vertex, if applicable, or <code>null</code>
   * @param hotPixelVertexIndex the index of the hotPixel vertex, if applicable, or -1
   * @return <code>true</code> if a node was added for this pixel
   */
  public boolean snap(HotPixel hotPixel, SegmentString parentEdge, int hotPixelVertexIndex)
  {
    final Envelope pixelEnv = hotPixel.getSafeEnvelope();
    final HotPixelSnapAction hotPixelSnapAction = new HotPixelSnapAction(hotPixel, parentEdge, hotPixelVertexIndex);

    index.query(pixelEnv, new ItemVisitor() {
      public void visitItem(Object item) {
        MonotoneChain testChain = (MonotoneChain) item;
        testChain.select(pixelEnv, hotPixelSnapAction);
      }
    }
    );
    return hotPixelSnapAction.isNodeAdded();
  }

  public boolean snap(HotPixel hotPixel)
  {
    return snap(hotPixel, null, -1);
  }

  public class HotPixelSnapAction
      extends MonotoneChainSelectAction
  {
    private HotPixel hotPixel;
    private SegmentString parentEdge;
    // is -1 if hotPixel is not a vertex
    private int hotPixelVertexIndex;
    private boolean isNodeAdded = false;

    public HotPixelSnapAction(HotPixel hotPixel, SegmentString parentEdge, int hotPixelVertexIndex)
    {
      this.hotPixel = hotPixel;
      this.parentEdge = parentEdge;
      this.hotPixelVertexIndex = hotPixelVertexIndex;
    }

    public boolean isNodeAdded() { return isNodeAdded; }

    public void select(MonotoneChain mc, int startIndex)
    {
    	NodedSegmentString ss = (NodedSegmentString) mc.getContext();
      /**
       * Check to avoid snapping a hotPixel vertex to the same vertex.
       * This method is called for segments which intersects the 
       * hot pixel,
       * so need to check if either end of the segment is equal to the hot pixel
       * and if so, do not snap.
       * 
       * Sep 22 2012 - MD - currently do need to snap to every vertex,
       * since otherwise the testCollapse1 test in SnapRoundingTest fails.
       */
      if (parentEdge != null) {
        if (ss == parentEdge && 
            (startIndex == hotPixelVertexIndex
                ))
          return;
      }
      isNodeAdded = hotPixel.addSnappedNode(ss, startIndex);
    }

  }

}
