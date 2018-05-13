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
 * N dimensional point used to signify the bounds of a HyperRect
 *
 * Created by jcairns on 5/5/15.
 */
public interface HyperPoint {

    /**
     * The number of dimensions represented by this point
     *
     * @return dimension count
     */
    int getNDim();

    /**
     * Get the value of this point in the given dimension
     *
     * @param d - dimension
     *
     * @param <D> - A comparable coordinate 
     *
     * @return D - value of this point in the dimension
     * @throws IllegalArgumentException if a non-existent dimension is requested
     */
    <D extends Comparable<D>> D getCoord(int d);

    /**
     * Calculate the distance from this point to the given point across all dimensions
     *
     * @param p - point to calculate distance to
     *
     * @return distance to the point
     * @throws IllegalArgumentException if a non-existent dimension is requested
     */
    double distance(HyperPoint p);

    /**
     * Calculate the distance from this point to the given point in a specific dimension
     *
     * @param p - point to calculate distance to
     * @param d - dimension to use in calculation
     *
     * @return distance to the point in the fiven dimension
     */
    double distance(HyperPoint p, int d);

}
