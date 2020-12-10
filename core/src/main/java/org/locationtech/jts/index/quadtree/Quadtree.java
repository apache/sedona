
/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.locationtech.jts.index.quadtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.ArrayListVisitor;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.SpatialIndex;
/**
 * A Quadtree is a spatial index structure for efficient range querying
 * of items bounded by 2D rectangles.  
 * {@link Geometry}s can be indexed by using their
 * {@link Envelope}s.
 * Any type of Object can also be indexed as
 * long as it has an extent that can be represented by an {@link Envelope}.
 * <p>
 * This Quadtree index provides a <b>primary filter</b>
 * for range rectangle queries.  The various query methods return a list of
 * all items which <i>may</i> intersect the query rectangle.  Note that
 * it may thus return items which do <b>not</b> in fact intersect the query rectangle.
 * A secondary filter is required to test for actual intersection 
 * between the query rectangle and the envelope of each candidate item. 
 * The secondary filter may be performed explicitly, 
 * or it may be provided implicitly by subsequent operations executed on the items 
 * (for instance, if the index query is followed by computing a spatial predicate 
 * between the query geometry and tree items, 
 * the envelope intersection check is performed automatically.
 * <p>
 * This implementation does not require specifying the extent of the inserted
 * items beforehand.  It will automatically expand to accommodate any extent
 * of dataset.
 * <p>
 * This data structure is also known as an <i>MX-CIF quadtree</i>
 * following the terminology of Samet and others.
 *
 * @version 1.7
 */
public class Quadtree
    implements SpatialIndex, Serializable
{
  private static final long serialVersionUID = -7461163625812743604L;

  /**
   * Ensure that the envelope for the inserted item has non-zero extents.
   * Use the current minExtent to pad the envelope, if necessary
   */
  public static Envelope ensureExtent(Envelope itemEnv, double minExtent)
  {
    //The names "ensureExtent" and "minExtent" are misleading -- sounds like
    //this method ensures that the extents are greater than minExtent.
    //Perhaps we should rename them to "ensurePositiveExtent" and "defaultExtent".
    //[Jon Aquino]
    double minx = itemEnv.getMinX();
    double maxx = itemEnv.getMaxX();
    double miny = itemEnv.getMinY();
    double maxy = itemEnv.getMaxY();
    // has a non-zero extent
    if (minx != maxx && miny != maxy) return itemEnv;

    // pad one or both extents
    if (minx == maxx) {
      minx = minx - minExtent / 2.0;
      maxx = maxx + minExtent / 2.0;
    }
    if (miny == maxy) {
      miny = miny - minExtent / 2.0;
      maxy = maxy + minExtent / 2.0;
    }
    return new Envelope(minx, maxx, miny, maxy);
  }

  private Root root;
  /**

  * minExtent is the minimum envelope extent of all items
  * inserted into the tree so far. It is used as a heuristic value
  * to construct non-zero envelopes for features with zero X and/or Y extent.
  * Start with a non-zero extent, in case the first feature inserted has
  * a zero extent in both directions.  This value may be non-optimal, but
  * only one feature will be inserted with this value.
  **/
  private double minExtent = 1.0;

  /**
   * Constructs a Quadtree with zero items.
   */
  public Quadtree()
  {
    root = new Root();
  }

  /**
   * Returns the number of levels in the tree.
   */
  public int depth()
  {
    //I don't think it's possible for root to be null. Perhaps we should
    //remove the check. [Jon Aquino]
    //Or make an assertion [Jon Aquino 10/29/2003]
    if (root != null) return root.depth();
    return 0;
  }

  /**
   * Tests whether the index contains any items.
   * 
   * @return true if the index does not contain any items
   */
  public boolean isEmpty()
  {
    if (root == null) return true;
    return root.isEmpty();
  }
  
  /**
   * Returns the number of items in the tree.
   *
   * @return the number of items in the tree
   */
  public int size()
  {
    if (root != null) return root.size();
    return 0;
  }

  public void insert(Envelope itemEnv, Object item)
  {
    collectStats(itemEnv);
    Envelope insertEnv = ensureExtent(itemEnv, minExtent);
    root.insert(insertEnv, item);
  }

  /**
   * Removes a single item from the tree.
   *
   * @param itemEnv the Envelope of the item to be removed
   * @param item the item to remove
   * @return <code>true</code> if the item was found (and thus removed)
   */
  public boolean remove(Envelope itemEnv, Object item)
  {
    Envelope posEnv = ensureExtent(itemEnv, minExtent);
    return root.remove(posEnv, item);
  }

/*
  public List OLDquery(Envelope searchEnv)
  {
    /**
     * the items that are matched are the items in quads which
     * overlap the search envelope
     */
    /*
    List foundItems = new ArrayList();
    root.addAllItemsFromOverlapping(searchEnv, foundItems);
    return foundItems;
  }
*/

  /**
   * Queries the tree and returns items which may lie in the given search envelope.
   * Precisely, the items that are returned are all items in the tree 
   * whose envelope <b>may</b> intersect the search Envelope.
   * Note that some items with non-intersecting envelopes may be returned as well;
   * the client is responsible for filtering these out.
   * In most situations there will be many items in the tree which do not
   * intersect the search envelope and which are not returned - thus
   * providing improved performance over a simple linear scan.    
   * 
   * @param searchEnv the envelope of the desired query area.
   * @return a List of items which may intersect the search envelope
   */
  public List query(Envelope searchEnv)
  {
    /**
     * the items that are matched are the items in quads which
     * overlap the search envelope
     */
    ArrayListVisitor visitor = new ArrayListVisitor();
    query(searchEnv, visitor);
    return visitor.getItems();
  }

  /**
   * Queries the tree and visits items which may lie in the given search envelope.
   * Precisely, the items that are visited are all items in the tree 
   * whose envelope <b>may</b> intersect the search Envelope.
   * Note that some items with non-intersecting envelopes may be visited as well;
   * the client is responsible for filtering these out.
   * In most situations there will be many items in the tree which do not
   * intersect the search envelope and which are not visited - thus
   * providing improved performance over a simple linear scan.    
   * 
   * @param searchEnv the envelope of the desired query area.
   * @param visitor a visitor object which is passed the visited items
   */
  public void query(Envelope searchEnv, ItemVisitor visitor)
  {
    /**
     * the items that are matched are the items in quads which
     * overlap the search envelope
     */
    root.visit(searchEnv, visitor);
  }

  /**
   * Return a list of all items in the Quadtree
   */
  public List queryAll()
  {
    List foundItems = new ArrayList();
    root.addAllItems(foundItems);
    return foundItems;
  }

  private void collectStats(Envelope itemEnv)
  {
    double delX = itemEnv.getWidth();
    if (delX < minExtent && delX > 0.0)
      minExtent = delX;

    double delY = itemEnv.getHeight();
    if (delY < minExtent && delY > 0.0)
      minExtent = delY;
  }

  Root getRoot()
  {
    return root;
  }
}
