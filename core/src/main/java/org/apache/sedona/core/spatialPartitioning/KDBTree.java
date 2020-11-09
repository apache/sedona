/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.spatialPartitioning;

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * see https://en.wikipedia.org/wiki/K-D-B-tree
 */
public class KDBTree
        implements Serializable
{

    private final int maxItemsPerNode;
    private final int maxLevels;
    private final Envelope extent;
    private final int level;
    private final List<Envelope> items = new ArrayList<>();
    private KDBTree[] children;
    private int leafId = 0;

    public KDBTree(int maxItemsPerNode, int maxLevels, Envelope extent)
    {
        this(maxItemsPerNode, maxLevels, 0, extent);
    }

    private KDBTree(int maxItemsPerNode, int maxLevels, int level, Envelope extent)
    {
        this.maxItemsPerNode = maxItemsPerNode;
        this.maxLevels = maxLevels;
        this.level = level;
        this.extent = extent;
    }

    public int getItemCount()
    {
        return items.size();
    }

    public boolean isLeaf()
    {
        return children == null;
    }

    public int getLeafId()
    {
        if (!isLeaf()) {
            throw new IllegalStateException();
        }

        return leafId;
    }

    public Envelope getExtent()
    {
        return extent;
    }

    public void insert(Envelope envelope)
    {
        if (items.size() < maxItemsPerNode || level >= maxLevels) {
            items.add(envelope);
        }
        else {
            if (children == null) {
                // Split over longer side
                boolean splitX = extent.getWidth() > extent.getHeight();
                boolean ok = split(splitX);
                if (!ok) {
                    // Try spitting by the other side
                    ok = split(!splitX);
                }

                if (!ok) {
                    // This could happen if all envelopes are the same.
                    items.add(envelope);
                    return;
                }
            }

            for (KDBTree child : children) {
                if (child.extent.contains(envelope.getMinX(), envelope.getMinY())) {
                    child.insert(envelope);
                    break;
                }
            }
        }
    }

    public void dropElements()
    {
        traverse(new Visitor()
        {
            @Override
            public boolean visit(KDBTree tree)
            {
                tree.items.clear();
                return true;
            }
        });
    }

    public List<KDBTree> findLeafNodes(final Envelope envelope)
    {
        final List<KDBTree> matches = new ArrayList<>();
        traverse(new Visitor()
        {
            @Override
            public boolean visit(KDBTree tree)
            {
                if (!disjoint(tree.getExtent(), envelope)) {
                    if (tree.isLeaf()) {
                        matches.add(tree);
                    }
                    return true;
                }
                else {
                    return false;
                }
            }
        });

        return matches;
    }

    private boolean disjoint(Envelope r1, Envelope r2)
    {
        return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     */
    public void traverse(Visitor visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (children != null) {
            for (KDBTree child : children) {
                child.traverse(visitor);
            }
        }
    }

    public void assignLeafIds()
    {
        traverse(new Visitor()
        {
            int id = 0;

            @Override
            public boolean visit(KDBTree tree)
            {
                if (tree.isLeaf()) {
                    tree.leafId = id;
                    id++;
                }
                return true;
            }
        });
    }

    private boolean split(boolean splitX)
    {
        final Comparator<Envelope> comparator = splitX ? new XComparator() : new YComparator();
        Collections.sort(items, comparator);

        final Envelope[] splits;
        final Splitter splitter;
        Envelope middleItem = items.get((int) Math.floor(items.size() / 2));
        if (splitX) {
            double x = middleItem.getMinX();
            if (x > extent.getMinX() && x < extent.getMaxX()) {
                splits = splitAtX(extent, x);
                splitter = new XSplitter(x);
            }
            else {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        }
        else {
            double y = middleItem.getMinY();
            if (y > extent.getMinY() && y < extent.getMaxY()) {
                splits = splitAtY(extent, y);
                splitter = new YSplitter(y);
            }
            else {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        }

        children = new KDBTree[2];
        children[0] = new KDBTree(maxItemsPerNode, maxLevels, level + 1, splits[0]);
        children[1] = new KDBTree(maxItemsPerNode, maxLevels, level + 1, splits[1]);

        // Move items
        splitItems(splitter);
        return true;
    }

    private void splitItems(Splitter splitter)
    {
        for (Envelope item : items) {
            children[splitter.split(item) ? 0 : 1].insert(item);
        }
    }

    private Envelope[] splitAtX(Envelope envelope, double x)
    {
        assert (envelope.getMinX() < x);
        assert (envelope.getMaxX() > x);
        Envelope[] splits = new Envelope[2];
        splits[0] = new Envelope(envelope.getMinX(), x, envelope.getMinY(), envelope.getMaxY());
        splits[1] = new Envelope(x, envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
        return splits;
    }

    private Envelope[] splitAtY(Envelope envelope, double y)
    {
        assert (envelope.getMinY() < y);
        assert (envelope.getMaxY() > y);
        Envelope[] splits = new Envelope[2];
        splits[0] = new Envelope(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), y);
        splits[1] = new Envelope(envelope.getMinX(), envelope.getMaxX(), y, envelope.getMaxY());
        return splits;
    }

    public interface Visitor
    {
        /**
         * Visits a single node of the tree
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(KDBTree tree);
    }

    private interface Splitter
    {
        /**
         * @return true if the specified envelope belongs to the lower split
         */
        boolean split(Envelope envelope);
    }

    private static final class XComparator
            implements Comparator<Envelope>
    {
        @Override
        public int compare(Envelope o1, Envelope o2)
        {
            double deltaX = o1.getMinX() - o2.getMinX();
            return (int) Math.signum(deltaX != 0 ? deltaX : o1.getMinY() - o2.getMinY());
        }
    }

    private static final class YComparator
            implements Comparator<Envelope>
    {
        @Override
        public int compare(Envelope o1, Envelope o2)
        {
            double deltaY = o1.getMinY() - o2.getMinY();
            return (int) Math.signum(deltaY != 0 ? deltaY : o1.getMinX() - o2.getMinX());
        }
    }

    private static final class XSplitter
            implements Splitter
    {
        private final double x;

        private XSplitter(double x)
        {
            this.x = x;
        }

        @Override
        public boolean split(Envelope envelope)
        {
            return envelope.getMinX() <= x;
        }
    }

    private static final class YSplitter
            implements Splitter
    {
        private final double y;

        private YSplitter(double y)
        {
            this.y = y;
        }

        @Override
        public boolean split(Envelope envelope)
        {
            return envelope.getMinY() <= y;
        }
    }
}
