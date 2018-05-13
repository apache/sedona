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
 * Node that will contain the data entries. Implemented by different type of SplitType leaf classes.
 *
 * Created by jcairns on 4/30/15.
 */
public abstract class Leaf<T> implements Node<T> {

    protected final int mMax;       // max entries per node

    protected final int mMin;       // least number of entries per node

    protected final RTree.Split splitType;

    protected final HyperRect[] r;

    protected final T[] entry;

    protected final RectBuilder<T> builder;

    protected HyperRect mbr;

    protected int size;

    public int partitioniId = -1;

    protected Leaf(final RectBuilder<T> builder, final int mMin, final int mMax, final RTree.Split splitType) {
        this.mMin = mMin;
        this.mMax = mMax;
        this.mbr = null;
        this.builder = builder;
        this.r  = new HyperRect[mMax];
        this.entry = (T[]) new Object[mMax];
        this.size = 0;
        this.splitType = splitType;
    }

    @Override
    public Node<T> add(final T t) {
        if(size < mMax) {
            final HyperRect tRect = builder.getBBox(t);
            if(mbr != null) {
                mbr = mbr.getMbr(tRect);
            } else {
                mbr = tRect;
            }

            r[size] = tRect;
            entry[size++] = t;
        } else {
            return split(t);
        }

        return this;
    }

    @Override
    public Node<T> remove(final T t)  {

        int i=0;
        int j;

        while(i<size && (entry[i]!=t) && (!entry[i].equals(t))) {
            i++;
        }

        j=i;

        while(j<size && ((entry[j]==t) || entry[j].equals(t))) {
            j++;
        }

        if(i < j) {
            final int nRemoved = j-i;
            if (j < size) {
                final int nRemaining = size-j;
                System.arraycopy(r, j, r, i, nRemaining);
                System.arraycopy(entry, j, entry, i, nRemaining);
                for (int k=size-nRemoved; k < size; k++) {
                    r[k] = null;
                    entry[k] = null;
                }
            } else {
                if(i==0) {
                    // clean sweep
                    return null;
                }
                for (int k=i; k < size; k++) {
                    r[k] = null;
                    entry[k] = null;
                }
            }

            size -= nRemoved;

            for(int k=0; k<size; k++) {
                if(k==0) {
                    mbr = r[k];
                } else {
                    mbr = mbr.getMbr(r[k]);
                }
            }

        }

        return this;

    }

    @Override
    public Node<T> update(final T told, final T tnew) {
        final HyperRect bbox = builder.getBBox(tnew);

        for(int i=0; i<size; i++) {
            if (entry[i].equals(told)) {
                r[i] = bbox;
                entry[i] = tnew;
            }

            if (i == 0) {
                mbr = r[i];
            } else {
                mbr = mbr.getMbr(r[i]);
            }
        }

        return this;
    }

    @Override
    public int search(final HyperRect rect, final T[] t, int n) {
        final int tLen = t.length;
        final int n0 = n;

        for(int i=0; i<size && n<tLen; i++) {
            if(rect.contains(r[i])) {
                t[n++] = entry[i];
            }
        }
        return n - n0;
    }

    public void search(final HyperRect rect, List<Leaf> t) {
        if (this.mbr.intersects(rect)) {
            t.add(this);
        }
        return;
    }


    public void assignPartitionId(AtomicInteger i) {
        this.partitioniId = i.incrementAndGet();
    }

    @Override
    public void search(HyperRect rect, Consumer<T> consumer) {
        for(int i = 0; i < size; i++) {
            if(rect.contains(r[i])) {
                consumer.accept(entry[i]);
            }
        }
    }

    @Override
    public int intersects(final HyperRect rect, final T[] t, int n) {
        final int tLen = t.length;
        final int n0 = n;

        for(int i=0; i<size && n<tLen; i++) {
            if(rect.intersects(r[i])) {
                t[n++] = entry[i];
            }
        }
        return n - n0;
    }

    @Override
    public void intersects(HyperRect rect, Consumer<T> consumer) {
        for(int i = 0; i < size; i++) {
            if(rect.intersects(r[i])) {
                consumer.accept(entry[i]);
            }
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int totalSize() {
        return size;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public HyperRect getBound() {
        return mbr;
    }

    static <R> Node<R> create(final RectBuilder<R> builder, final int mMin, final int M, final RTree.Split splitType) {

        switch(splitType) {
            case LINEAR:
                return new LinearSplitLeaf<>(builder, mMin, M);
            case QUADRATIC:
                return new QuadraticSplitLeaf<>(builder, mMin, M);
            case AXIAL:
            default:
                return new AxialSplitLeaf<>(builder, mMin, M);

        }

    }

    /**
     * Splits a leaf node that has the maximum number of entries into 2 leaf nodes of the same type with half
     * of the entries in each one.
     *
     * @param t entry to be added to the full leaf node
     * @return newly created node storing half the entries of this node
     */
    protected abstract Node<T> split(final T t);

    @Override
    public void forEach(Consumer<T> consumer) {
        for(int i = 0; i < size; i++) {
            consumer.accept(entry[i]);
        }
    }

    @Override
    public boolean contains(HyperRect rect, T t) {
        for(int i = 0; i < size; i++) {
            if(rect.contains(r[i])) {
                if(entry[i].equals(t)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void collectStats(Stats stats, int depth) {
        if (depth > stats.getMaxDepth()) {
            stats.setMaxDepth(depth);
        }
        stats.countLeafAtDepth(depth);
        stats.countEntriesAtDepth(size, depth);
    }

    /**
     * Figures out which newly made leaf node (see split method) to add a data entry to.
     *
     * @param l1Node left node
     * @param l2Node right node
     * @param t data entry to be added
     */
    protected final void classify(final Node<T> l1Node, final Node<T> l2Node, final T t) {
        final HyperRect tRect = builder.getBBox(t);
        final HyperRect l1Mbr = l1Node.getBound().getMbr(tRect);
        final HyperRect l2Mbr = l2Node.getBound().getMbr(tRect);
        final double l1CostInc = Math.max(l1Mbr.cost() - (l1Node.getBound().cost() + tRect.cost()), 0.0);
        final double l2CostInc = Math.max(l2Mbr.cost() - (l2Node.getBound().cost() + tRect.cost()), 0.0);
        if(l2CostInc > l1CostInc) {
            l1Node.add(t);
        } else if(RTree.isEqual(l1CostInc, l2CostInc)) {
            final double l1MbrCost = l1Mbr.cost();
            final double l2MbrCost = l2Mbr.cost();
            if(l1MbrCost < l2MbrCost) {
                l1Node.add(t);
            } else if(RTree.isEqual(l1MbrCost, l2MbrCost)) {
                final double l1MbrMargin = l1Mbr.perimeter();
                final double l2MbrMargin = l2Mbr.perimeter();
                if(l1MbrMargin < l2MbrMargin) {
                    l1Node.add(t);
                } else if(RTree.isEqual(l1MbrMargin, l2MbrMargin)) {
                    // break ties with least number
                    if (l1Node.size() < l2Node.size()) {
                        l1Node.add(t);
                    } else {
                        l2Node.add(t);
                    }
                } else {
                    l2Node.add(t);
                }
            } else {
                l2Node.add(t);
            }
        }
        else {
            l2Node.add(t);
        }

    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(128);
        sb.append(splitType.name());
        sb.append('[');
        sb.append(mbr);
        sb.append(']');

        return sb.toString();
    }

    @Override
    public Node<T> instrument() {
        return new CounterNode<>(this);
    }
}
