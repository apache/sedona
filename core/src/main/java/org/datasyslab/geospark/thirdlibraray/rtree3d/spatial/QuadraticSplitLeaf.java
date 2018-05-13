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

/**
 * Guttmann's Quadratic split
 *
 * Created by jcairns on 5/5/15.
 */
final class QuadraticSplitLeaf<T> extends Leaf<T> {

    protected QuadraticSplitLeaf(final RectBuilder<T> builder, final int mMin, final int mMax) {
        super(builder, mMin, mMax, RTree.Split.QUADRATIC);
    }

    @Override
    protected Node<T> split(final T t) {

        final Branch<T> pNode = new Branch<>(builder, mMin, mMax, splitType);
        final Node<T> l1Node = create(builder, mMin, mMax, splitType);
        final Node<T> l2Node = create(builder, mMin, mMax, splitType);

        // find the two rectangles that are most wasteful
        double minCost = Double.MIN_VALUE;
        int r1Max=0, r2Max=size-1;
        for(int i=0; i<size; i++) {
            for(int j=i+1; j<size; j++) {
                final HyperRect mbr = r[i].getMbr(r[j]);
                final double cost = mbr.cost() - (r[i].cost() + r[j].cost());
                if(cost > minCost) {
                    r1Max = i;
                    r2Max = j;
                    minCost = cost;
                }
            }
        }

        // two seeds
        l1Node.add(entry[r1Max]);
        l2Node.add(entry[r2Max]);

        for(int i=0; i<size; i++) {
            if((i != r1Max) && (i != r2Max)) {
                // classify with respect to nodes
                classify(l1Node, l2Node, entry[i]);
            }
        }

        classify(l1Node, l2Node, t);

        pNode.addChild(l1Node);
        pNode.addChild(l2Node);

        return pNode;
    }

}
