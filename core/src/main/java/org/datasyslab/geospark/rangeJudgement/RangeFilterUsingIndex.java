/*
 * FILE: RangeFilterUsingIndex
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.rangeJudgement;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.SpatialIndex;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

public class RangeFilterUsingIndex<U extends Geometry, T extends Geometry>
        extends JudgementBase
        implements FlatMapFunction<Iterator<SpatialIndex>, T>
{

    public RangeFilterUsingIndex(U queryWindow, boolean considerBoundaryIntersection, boolean leftCoveredByRight)
    {
        super(queryWindow, considerBoundaryIntersection, leftCoveredByRight);
    }

    /**
     * Call.
     *
     * @param treeIndexes the tree indexes
     * @return the iterator
     * @throws Exception the exception
     */
    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    @Override
    public Iterator<T> call(Iterator<SpatialIndex> treeIndexes)
            throws Exception
    {
        assert treeIndexes.hasNext() == true;
        SpatialIndex treeIndex = treeIndexes.next();
        List<T> results = new ArrayList<T>();
        List<T> tempResults = treeIndex.query(this.queryGeometry.getEnvelopeInternal());
        for (T tempResult : tempResults) {
            if (leftCoveredByRight) {
                if (match(tempResult, queryGeometry)) {
                    results.add(tempResult);
                }
            }
            else {
                if (match(queryGeometry, tempResult)) {
                    results.add(tempResult);
                }
            }
        }
        return results.iterator();
    }
}
