package org.datasyslab.geospark.thirdlibraray.rtree3d.spatial;

/*
 * #%L
 * Conversant RTree
 * ~~
 * Conversantmedia.com © 2016, Conversant, Inc. Conversant® is a trademark of Conversant, Inc.
 * ~~
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * <p>Data structure to make range searching more efficient. Indexes multi-dimensional information
 * such as geographical coordinates or rectangles. Groups information and represents them with a
 * minimum bounding rectangle (mbr). When searching through the tree, any query that does not
 * intersect an mbr can ignore any data entries in that mbr.</p>
 * <p>More information can be found here @see <a href="https://en.wikipedia.org/wiki/R-tree">https://en.wikipedia.org/wiki/R-tree</a></p>
 * <p>
 * Created by jcairns on 4/30/15.</p>
 */
public final class RTree<T> implements SpatialSearch<T> {
    private static final double EPSILON = 1e-12;

    private final int mMin;
    private final int mMax;
    private final RectBuilder<T> builder;
    private final Split splitType;

    private Node<T> root = null;

    public RTree(final RectBuilder<T> builder, final int mMin, final int mMax, final Split splitType) {
        this.mMin = mMin;
        this.mMax = mMax;
        this.builder = builder;
        this.splitType = splitType;
    }

    @Override
    public int search(final HyperRect rect, final T[] t) {
        if(root != null) {
            return root.search(rect, t, 0);
        }
        return 0;
    }

    public void search(final HyperRect rect, List<Leaf> t) {
        if (root != null) {
            root.search(rect, t);
        }
    }

    @Override
    public void search(HyperRect rect, Consumer<T> consumer) {
        if(root != null) {
            root.search(rect, consumer);
        }
    }

    @Override
    public int intersects(final HyperRect rect, final T[] t) {
        if(root != null) {
            return root.intersects(rect, t, 0);
        }
        return 0;
    }

    @Override
    public void intersects(HyperRect rect, Consumer<T> consumer) {
        if(root != null) {
            root.intersects(rect, consumer);
        }
    }

    @Override
    public void add(final T t) {
        if(root != null) {
            root = root.add(t);
        } else {
            root = Leaf.create(builder, mMin, mMax, splitType);
            root.add(t);
        }
    }

    @Override
    public void remove(final T t) {
        if(root != null) {
            root = root.remove(t);
        }
    }

    @Override
    public void update(final T told, final T tnew) {
        if(root != null) {
            root = root.update(told, tnew);
        }
    }

    @Override
    public int getEntryCount() {
        if(root  != null) {
            return root.totalSize();
        }
        return 0;
    }

    public void assignPartitionId() {
        AtomicInteger i = new AtomicInteger();
        if (root != null) {
            root.assignPartitionId(i);
        }
    }

    /**
     * returns whether or not the HyperRect will enclose all of the data entries in t
     *
     * @param t    Data entries to be evaluated
     *
     * @return boolean - Whether or not all entries lie inside rect
     */
    @Override
    public boolean contains(final T t) {
        if(root != null) {
            final HyperRect bbox = builder.getBBox(t);
            return root.contains(bbox, t);
        }
        return false;
    }

    public static boolean isEqual(final double a, final double b) {
        return isEqual(a, b, EPSILON);
    }

    static boolean isEqual(final double a, final double b, final double eps) {
        return Math.abs(a - b) <= ((Math.abs(a) < Math.abs(b) ? Math.abs(b) : Math.abs(a)) * eps);
    }

    @Override
    public void forEach(Consumer<T> consumer) {
        if(root != null) {
            root.forEach(consumer);
        }
    }

    public void instrumentTree() {
        if(root != null) {
            root = root.instrument();
            ((CounterNode<T>) root).searchCount = 0;
            ((CounterNode<T>) root).bboxEvalCount = 0;
        }
    }

    @Override
    public Stats collectStats() {
        Stats stats = new Stats();
        stats.setType(splitType);
        stats.setMaxFill(mMax);
        stats.setMinFill(mMin);
        root.collectStats(stats, 0);
        return stats;
    }

    Node<T> getRoot() {
        return root;
    }


    /**
     * Different methods for splitting nodes in an RTree.
     *
     * AXIAL has been shown to give good performance for many general spatial problems,
     *
     * <p>
     * Created by ewhite on 10/28/15.
     */
    public enum Split {
        AXIAL,
        LINEAR,
        QUADRATIC,
    }
}
