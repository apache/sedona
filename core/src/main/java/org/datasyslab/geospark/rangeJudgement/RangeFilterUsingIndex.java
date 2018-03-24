/**
 * FILE: RangeFilterUsingIndex.java
 * PATH: org.datasyslab.geospark.rangeJudgement.RangeFilterUsingIndex.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
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
