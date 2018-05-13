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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Create instances of SpatialSearch implementations
 *
 * Created by jcovert on 2/3/16.
 */
public class SpatialSearches {

    private static final int DEFAULT_MIN_M = 2;
    private static final int DEFAULT_MAX_M = 8;
    private static final RTree.Split DEFAULT_SPLIT_TYPE = RTree.Split.AXIAL;

    private SpatialSearches() {}

    /**
     * Create an R-Tree with default values for m, M, and split type
     *
     * @param builder - Builder implementation used to create HyperRects out of T's
     * @param <T> - The store type of the bound
     *
     * @return SpatialSearch - The spatial search and index structure
     */
    public static <T> SpatialSearch<T> rTree(final RectBuilder<T> builder) {
        return new RTree<>(builder, DEFAULT_MIN_M, DEFAULT_MAX_M, DEFAULT_SPLIT_TYPE);
    }

    /**
     * Create an R-Tree with specified values for m, M, and split type
     *
     * @param builder - Builder implementation used to create HyperRects out of T's
     * @param minM - minimum number of entries per node of this tree
     * @param maxM - maximum number of entries per node of this tree (exceeding this causes node split)
     * @param splitType - type of split to use when M+1 entries are added to a node
     * @param <T> - The store type of the bound
     *
     * @return SpatialSearch - The spatial search and index structure
     */
    public static <T> SpatialSearch<T> rTree(final RectBuilder<T> builder, final int minM, final int maxM, final RTree.Split splitType) {
        return new RTree<>(builder, minM, maxM, splitType);
    }

    /**
     * Create a protected R-Tree with default values for m, M, and split type
     *
     * @param builder - Builder implementation used to create HyperRects out of T's
     * @param <T> - The store type of the bound
     *
     * @return SpatialSearch - The spatial search and index structure
     */
    public static <T> SpatialSearch<T> lockingRTree(final RectBuilder<T> builder) {
        return new ConcurrentRTree<>(rTree(builder), new ReentrantReadWriteLock(true));
    }

    /**
     * Create a protected R-Tree with specified values for m, M, and split type
     *
     * @param builder - Builder implementation used to create HyperRects out of T's
     * @param minM - minimum number of entries per node of this tree
     * @param maxM - maximum number of entries per node of this tree (exceeding this causes node split)
     * @param splitType - type of split to use when M+1 entries are added to a node
     * @param <T> - The store type of the bound
     *
     * @return SpatialSearch - The spatial search and index structure
     */
    public static <T> SpatialSearch<T> lockingRTree(final RectBuilder<T> builder, final int minM, final int maxM, final RTree.Split splitType) {
        return new ConcurrentRTree<>(rTree(builder, minM, maxM, splitType), new ReentrantReadWriteLock(true));
    }

}
