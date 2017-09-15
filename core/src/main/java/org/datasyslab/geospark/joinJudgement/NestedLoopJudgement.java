/**
 * FILE: NestedLoopJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.NestedLoopJudgement.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NestedLoopJudgement<T extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<Polygon>, Pair<Polygon, T>>, Serializable {

    /**
     * Instantiates a new geometry by polygon judgement.
     *
     * @param considerBoundaryIntersection the consider boundary intersection
     */
    public NestedLoopJudgement(boolean considerBoundaryIntersection) {
        super(considerBoundaryIntersection);
    }

    @Override
    public Iterator<Pair<Polygon, T>> call(Iterator<T> iteratorObject, Iterator<Polygon> iteratorWindow) throws Exception {
        List<Pair<Polygon, T>> result = new ArrayList<>();
        List<T> queryObjects = new ArrayList<>();
        while(iteratorObject.hasNext())
        {
            queryObjects.add(iteratorObject.next());
        }
        while (iteratorWindow.hasNext()) {
            Polygon window = iteratorWindow.next();
            for (int i =0;i<queryObjects.size();i++) {
                T object = queryObjects.get(i);
                if (match(window, object)) {
                    result.add(Pair.of(window, object));
                }
            }
        }
        return result.iterator();
    }
}
