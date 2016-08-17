
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
package com.vividsolutions.jts.index.quadtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.ItemVisitor;

/**
 * The base class for nodes in a {@link Quadtree}.
 *
 * @version 1.7
 */
public abstract class NodeBase implements Serializable {

//DEBUG private static int itemCount = 0;  // debugging
  
  /**
   * Gets the index of the subquad that wholly contains the given envelope.
   * If none does, returns -1.
   * 
   * @return the index of the subquad that wholly contains the given envelope
   * or -1 if no subquad wholly contains the envelope
   */
  public static int getSubnodeIndex(Envelope env, double centrex, double centrey)
  {
    int subnodeIndex = -1;
    if (env.getMinX() >= centrex) {
      if (env.getMinY() >= centrey) subnodeIndex = 3;
      if (env.getMaxY() <= centrey) subnodeIndex = 1;
    }
    if (env.getMaxX() <= centrex) {
      if (env.getMinY() >= centrey) subnodeIndex = 2;
      if (env.getMaxY() <= centrey) subnodeIndex = 0;
    }
    return subnodeIndex;
  }

  protected List items = new ArrayList();

  /**
   * subquads are numbered as follows:
   * <pre>
   *  2 | 3
   *  --+--
   *  0 | 1
   * </pre>
   */
  protected Node[] subnode = new Node[4];

  public NodeBase() {
  }

  public List getItems() { return items; }

  public boolean hasItems() { return ! items.isEmpty(); }

  public void add(Object item)
  {
    items.add(item);
//DEBUG itemCount++;
//DEBUG System.out.print(itemCount);
  }

  /**
   * Removes a single item from this subtree.
   *
   * @param itemEnv the envelope containing the item
   * @param item the item to remove
   * @return <code>true</code> if the item was found and removed
   */
  public boolean remove(Envelope itemEnv, Object item)
  {
    // use envelope to restrict nodes scanned
    if (! isSearchMatch(itemEnv))
      return false;

    boolean found = false;
    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null) {
        found = subnode[i].remove(itemEnv, item);
        if (found) {
          // trim subtree if empty
          if (subnode[i].isPrunable())
            subnode[i] = null;
          break;
        }
      }
    }
    // if item was found lower down, don't need to search for it here
    if (found) return found;
    // otherwise, try and remove the item from the list of items in this node
    found = items.remove(item);
    return found;
  }

  public boolean isPrunable()
  {
    return ! (hasChildren() || hasItems());
  }

  public boolean hasChildren()
  {
    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null)
        return true;
    }
    return false;
  }

  public boolean isEmpty()
  {
    boolean isEmpty = true;
    if (! items.isEmpty()) isEmpty = false;
    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null) {
        if (! subnode[i].isEmpty() )
          isEmpty = false;
      }
    }
    return isEmpty;
  }

  //<<TODO:RENAME?>> Sounds like this method adds resultItems to items
  //(like List#addAll). Perhaps it should be renamed to "addAllItemsTo" [Jon Aquino]
  public List addAllItems(List resultItems)
  {
    // this node may have items as well as subnodes (since items may not
    // be wholely contained in any single subnode
    resultItems.addAll(this.items);
    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null) {
        subnode[i].addAllItems(resultItems);
      }
    }
    return resultItems;
  }
  protected abstract boolean isSearchMatch(Envelope searchEnv);

  public void addAllItemsFromOverlapping(Envelope searchEnv, List resultItems)
  {
    if (! isSearchMatch(searchEnv))
      return;

    // this node may have items as well as subnodes (since items may not
    // be wholely contained in any single subnode
    resultItems.addAll(items);

    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null) {
        subnode[i].addAllItemsFromOverlapping(searchEnv, resultItems);
      }
    }
  }

  public void visit(Envelope searchEnv, ItemVisitor visitor)
  {
    if (! isSearchMatch(searchEnv))
      return;

    // this node may have items as well as subnodes (since items may not
    // be wholely contained in any single subnode
    visitItems(searchEnv, visitor);

    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null) {
        subnode[i].visit(searchEnv, visitor);
      }
    }
  }

  private void visitItems(Envelope searchEnv, ItemVisitor visitor)
  {
    // would be nice to filter items based on search envelope, but can't until they contain an envelope
    for (Iterator i = items.iterator(); i.hasNext(); ) {
      visitor.visitItem(i.next());
    }
  }

//<<TODO:RENAME?>> In Samet's terminology, I think what we're returning here is
//actually level+1 rather than depth. (See p. 4 of his book) [Jon Aquino]
  int depth()
  {
    int maxSubDepth = 0;
    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null) {
        int sqd = subnode[i].depth();
        if (sqd > maxSubDepth)
          maxSubDepth = sqd;
      }
    }
    return maxSubDepth + 1;
  }

  int size()
  {
    int subSize = 0;
    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null) {
        subSize += subnode[i].size();
      }
    }
    return subSize + items.size();
  }

  int getNodeCount()
  {
    int subSize = 0;
    for (int i = 0; i < 4; i++) {
      if (subnode[i] != null) {
        subSize += subnode[i].size();
      }
    }
    return subSize + 1;
  }
 

/**
 * Visit all boundaries of all subnodes
 * @param boundary
 * @param visitor
 */
public void queryBoundary(Envelope boundary, ItemVisitor visitor)
  {


    //This flag marks whether this node is containing any subnodes. Once we find one subnode exists, 
	//set the flag to true.
	boolean hasSubnodes=false;

    for (int i = 0; i < 4; i++) {
        if (subnode[i] != null) {

        	hasSubnodes=true;
        }
      }
    //If we find no subnodes, we know this is a leaf node. Record the boundary of this leaf node and return.
    if(hasSubnodes==false)
    {
    	visitor.visitItem(boundary);
    	return;
    }
    //If we can go here, we find this node is a non-leaf node. We should go deeper.
    for (int i = 0; i < 4; i++) {
        if (subnode[i] != null) {

        	hasSubnodes=true;
        	subnode[i].queryBoundary(subnode[i].getEnvelope(),visitor);
        }
      }


  }

}
