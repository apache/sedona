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
 * An N dimensional rectangle or "hypercube" that is a representation of a data entry.
 *
 * Created by jcairns on 4/30/15.
 */
public interface HyperRect<D extends Comparable<D>> {

    /**
     * Calculate the resulting mbr when combining param HyperRect with this HyperRect
     *
     * @param r - mbr to add
     *
     * @return new HyperRect representing mbr of both HyperRects combined
     */
    HyperRect getMbr(HyperRect r);

    /**
     * Get number of dimensions used in creating the HyperRect
     *
     * @return number of dimensions
     */
    int getNDim();

    /**
     * Get the minimum HyperPoint of this HyperRect
     *
     * @return  min HyperPoint
     */
    HyperPoint getMin();

    /**
     * Get the minimum HyperPoint of this HyperRect
     *
     * @return  min HyperPoint
     */
    HyperPoint getMax();

    /**
     * Get the HyperPoint representing the center point in all dimensions of this HyperRect
     *
     * @return  middle HyperPoint
     */
    HyperPoint getCentroid();

    /**
     * Calculate the distance between the min and max HyperPoints in given dimension
     *
     * @param d - dimension to calculate
     *
     * @return double - the numeric range of the dimention (min - max)
     */
    double getRange(final int d);

    /**
     * Determines if this HyperRect fully encloses parameter HyperRect
     *
     * @param r - HyperRect to test
     *
     * @return true if contains, false otherwise
     */
    boolean contains(HyperRect r);

    /**
     * Determines if this HyperRect intersects parameter HyperRect on any axis
     *
     * @param r - HyperRect to test
     *
     * @return true if intersects, false otherwise
     */
    boolean intersects(HyperRect r);

    /**
     * Calculate the "cost" of this HyperRect - usually the area across all dimensions
     *
     * @return - cost
     */
    double cost();

    /**
     * Calculate the perimeter of this HyperRect - across all dimesnions
     *
     * @return - perimeter
     */
    double perimeter();
}
