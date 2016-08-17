
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
package com.vividsolutions.jts.index.bintree;

import com.vividsolutions.jts.util.Assert;

/**
 * A node of a {@link Bintree}.
 *
 * @version 1.7
 */
public class Node
  extends NodeBase
{
  public static Node createNode(Interval itemInterval)
  {
    Key key = new Key(itemInterval);

//System.out.println("input: " + env + "  binaryEnv: " + key.getEnvelope());
    Node node = new Node(key.getInterval(), key.getLevel());
    return node;
  }

  public static Node createExpanded(Node node, Interval addInterval)
  {
    Interval expandInt = new Interval(addInterval);
    if (node != null) expandInt.expandToInclude(node.interval);

    Node largerNode = createNode(expandInt);
    if (node != null) largerNode.insert(node);
    return largerNode;
  }

  private Interval interval;
  private double centre;
  private int level;

  public Node(Interval interval, int level)
  {
    this.interval = interval;
    this.level = level;
    centre = (interval.getMin() + interval.getMax()) / 2;
  }

  public Interval getInterval() { return interval; }

  protected boolean isSearchMatch(Interval itemInterval)
  {
//    System.out.println(itemInterval + " overlaps " + interval + " : "
//                       + itemInterval.overlaps(interval));
    return itemInterval.overlaps(interval);
  }

  /**
   * Returns the subnode containing the envelope.
   * Creates the node if
   * it does not already exist.
   */
  public Node getNode(Interval searchInterval)
  {
    int subnodeIndex = getSubnodeIndex(searchInterval, centre);
    // if index is -1 searchEnv is not contained in a subnode
    if (subnodeIndex != -1) {
      // create the node if it does not exist
      Node node = getSubnode(subnodeIndex);
      // recursively search the found/created node
      return node.getNode(searchInterval);
    }
    else {
      return this;
    }
  }

  /**
   * Returns the smallest <i>existing</i>
   * node containing the envelope.
   */
  public NodeBase find(Interval searchInterval)
  {
    int subnodeIndex = getSubnodeIndex(searchInterval, centre);
    if (subnodeIndex == -1)
      return this;
    if (subnode[subnodeIndex] != null) {
      // query lies in subnode, so search it
      Node node = subnode[subnodeIndex];
      return node.find(searchInterval);
    }
    // no existing subnode, so return this one anyway
    return this;
  }

  void insert(Node node)
  {
    Assert.isTrue(interval == null || interval.contains(node.interval));
    int index = getSubnodeIndex(node.interval, centre);
    if (node.level == level - 1) {
      subnode[index] = node;
    }
    else {
      // the node is not a direct child, so make a new child node to contain it
      // and recursively insert the node
      Node childNode = createSubnode(index);
      childNode.insert(node);
      subnode[index] = childNode;
    }
  }

  /**
   * get the subnode for the index.
   * If it doesn't exist, create it
   */
  private Node getSubnode(int index)
  {
    if (subnode[index] == null) {
      subnode[index] = createSubnode(index);
    }
    return subnode[index];
  }

  private Node createSubnode(int index)
  {
        // create a new subnode in the appropriate interval

      double min = 0.0;
      double max = 0.0;

      switch (index) {
      case 0:
        min = interval.getMin();
        max = centre;
        break;
      case 1:
        min = centre;
        max = interval.getMax();
        break;
      }
      Interval subInt = new Interval(min, max);
      Node node = new Node(subInt, level - 1);
    return node;
  }

}
