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
 * RTree node that contains leaf nodes
 *
 * Created by jcairns on 4/30/15.
 */
final class Branch<T> implements Node<T> {

    private final RectBuilder<T> builder;

    private final int mMax;

    private final int mMin;

    private final RTree.Split splitType;

    private final Node[] child;

    private HyperRect mbr;

    private int size;

    Branch(final RectBuilder<T> builder, final int mMin, final int mMax, final RTree.Split splitType) {
        this.mMin = mMin;
        this.mMax = mMax;
        this.builder = builder;
        this.mbr = null;
        this.size = 0;
        this.child = new Node[mMax];
        this.splitType = splitType;
    }

    /**
     * Add a new node to this branch's list of children
     *
     * @param n node to be added (can be leaf or branch)
     * @return position of the added node
     */
    protected int addChild(final Node<T> n) {
        if(size < mMax) {
            child[size++] = n;

            if(mbr != null) {
                mbr = mbr.getMbr(n.getBound());
            } else {
                mbr = n.getBound();
            }

            return size - 1;
        }
        else {
            throw new RuntimeException("Too many children");
        }
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public HyperRect getBound() {
        return mbr;
    }

    /**
     * Adds a data entry to one of the child nodes of this branch
     *
     * @param t data entry to add
     * @return Node that the entry was added to
     */
    @Override
    public Node<T> add(final T t) {
        final HyperRect tRect = builder.getBBox(t);
        if(size < mMin) {
            for(int i=0; i<size; i++) {
                if(child[i].getBound().contains(tRect)) {
                    child[i] = child[i].add(t);
                    mbr = mbr.getMbr(child[i].getBound());
                    return this;
                }
            }
            // no overlapping node - grow
            final Node<T> nextLeaf = Leaf.create(builder, mMin, mMax, splitType);
            nextLeaf.add(t);
            final int nextChild = addChild(nextLeaf);
            mbr = mbr.getMbr(child[nextChild].getBound());

            return this;

        } else {
            final int bestLeaf = chooseLeaf(t, tRect);

            child[bestLeaf] = child[bestLeaf].add(t);
            mbr = mbr.getMbr(child[bestLeaf].getBound());

            return this;
        }
    }

    @Override
    public Node<T> remove(final T t) {
        final HyperRect tRect = builder.getBBox(t);

        for (int i = 0; i < size; i++) {
            if (child[i].getBound().intersects(tRect)) {
                child[i] = child[i].remove(t);

                if (child[i] == null) {
                    System.arraycopy(child, i+1, child, i, size-i-1);
                    size--;
                    child[size] = null;
                    if(size > 0) i--;
                }
            }
        }

        if (size == 0) {
            return null;
        } else if (size == 1) {
            // unsplit branch
            return child[0];
        }

        mbr = child[0].getBound();
        for(int i=1; i<size; i++) {
            mbr = mbr.getMbr(child[i].getBound());
        }

        return this;
    }

    @Override
    public Node<T> update(final T told, final T tnew) {
        final HyperRect tRect = builder.getBBox(told);
        for(int i = 0; i < size; i++){
            if(tRect.intersects(child[i].getBound())) {
                child[i] = child[i].update(told, tnew);
            }
            if(i==0) {
                mbr = child[i].getBound();
            } else {
                mbr = mbr.getMbr(child[i].getBound());
            }
        }
        return this;
    }

    @Override
    public void search(HyperRect rect, Consumer<T> consumer) {
        for(int i = 0; i < size; i++) {
            if(rect.intersects(child[i].getBound())) {
                child[i].search(rect, consumer);
            }
        }
    }

    public void assignPartitionId(AtomicInteger id) {
        for(int i = 0; i < size; i++) {
            child[i].assignPartitionId(id);
        }
    }

    @Override
    public int search(final HyperRect rect, final T[] t, int n) {
        final int tLen = t.length;
        final int n0 = n;
        for(int i=0; i < size && n < tLen; i++) {
            if (rect.intersects(child[i].getBound())) {
                n += child[i].search(rect, t, n);
            }
        }
        return n-n0;
    }

    public void search(final HyperRect rect, List<Leaf> t) {
        for (int i = 0; i < size; i++) {
            if (rect.intersects(child[i].getBound())) {
                child[i].search(rect, t);
            }
        }
        return;
    }

    @Override
    public void intersects(HyperRect rect, Consumer<T> consumer) {
        for(int i = 0; i < size; i++) {
            if(rect.intersects(child[i].getBound())) {
                child[i].intersects(rect, consumer);
            }
        }
    }

    @Override
    public int intersects(final HyperRect rect, final T[] t, int n) {
        final int tLen = t.length;
        final int n0 = n;
        for(int i=0; i < size && n < tLen; i++) {
            if (rect.intersects(child[i].getBound())) {
                n += child[i].intersects(rect, t, n);
            }
        }
        return n-n0;
    }

    /**
     * @return number of child nodes
     */
    @Override
    public int size() {
        return size;
    }

    @Override
    public int totalSize() {
        int s = 0;
        for(int i=0; i<size; i++) {
            s+= child[i].totalSize();
        }
        return s;
    }

    private int chooseLeaf(final T t, final HyperRect tRect) {
        if(size > 0) {
            int bestNode = 0;
            HyperRect childMbr = child[0].getBound().getMbr(tRect);
            double leastEnlargement = childMbr.cost() - (child[0].getBound().cost() + tRect.cost());
            double leastPerimeter   = childMbr.perimeter();

            for(int i = 1; i<size; i++) {
                childMbr = child[i].getBound().getMbr(tRect);
                final double nodeEnlargement = childMbr.cost() - (child[i].getBound().cost() + tRect.cost());
                if (nodeEnlargement < leastEnlargement) {
                    leastEnlargement = nodeEnlargement;
                    leastPerimeter  = childMbr.perimeter();
                    bestNode = i;
                }
                else if(RTree.isEqual(nodeEnlargement, leastEnlargement)) {
                    final double childPerimeter = childMbr.perimeter();
                    if (childPerimeter < leastPerimeter) {
                        leastEnlargement = nodeEnlargement;
                        leastPerimeter = childPerimeter;
                        bestNode = i;
                    }
                } // else its not the least

            }
            return bestNode;
        }
        else {
            final Node<T> n = Leaf.create(builder, mMin, mMax, splitType);
            n.add(t);
            child[size++] = n;

            if(mbr == null) {
                mbr = n.getBound();
            }
            else {
                mbr = mbr.getMbr(n.getBound());
            }

            return size-1;
        }
    }

    /**
     * Return child nodes of this branch.
     *
     * @return array of child nodes (leaves or branches)
     */
    public Node[] getChildren() {
        return child;
    }

    @Override
    public void forEach(Consumer<T> consumer) {
        for(int i = 0; i < size; i++) {
            child[i].forEach(consumer);
        }
    }

    @Override
    public boolean contains(HyperRect rect, T t) {
        for(int i = 0; i < size; i++) {
            if(rect.intersects(child[i].getBound())) {
                child[i].contains(rect, t);
            }
        }
        return false;
    }

    @Override
    public void collectStats(Stats stats, int depth) {
        for(int i = 0; i < size; i++) {
            child[i].collectStats(stats, depth + 1);
        }
        stats.countBranchAtDepth(depth);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(128);
        sb.append("BRANCH[");
        sb.append(mbr);
        sb.append(']');

        return sb.toString();
    }

    @Override
    public Node<T> instrument() {
        for(int i = 0; i < size; i++) {
            child[i] = child[i].instrument();
        }
        return new CounterNode<>(this);
    }
}
