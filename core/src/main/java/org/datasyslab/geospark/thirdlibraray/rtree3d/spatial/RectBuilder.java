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
 * Created by jcairns on 4/30/15.
 */
public interface RectBuilder<T> {

    /**
     * Build a bounding rectangle for the given element
     *
     * @param t - element to bound
     *
     * @return HyperRect impl for this entry
     */
    HyperRect getBBox(T t);


    /**
     * Build a bounding rectangle for given points (min and max, usually)
     *
     * @param p1 - first point (top-left point, for example)
     * @param p2 - second point (bottom-right point, for example)
     *
     * @return HyperRect impl defined by two points
     */
    HyperRect getMbr(HyperPoint p1, HyperPoint p2);
}
