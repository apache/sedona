/**
 * FILE: GeometryByCircleJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByCircleJudgement.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.geometryObjects.PairGeometry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;


// TODO: Auto-generated Javadoc

/**
 * The Class GeometryByCircleJudgement.
 */
public class GeometryByCircleJudgement<T extends Geometry> implements FlatMapFunction2<Iterator<T>, Iterator<Circle>, PairGeometry<Circle, T>>, Serializable {

    /**
     * The consider boundary intersection.
     */
    boolean considerBoundaryIntersection = false;

    /**
     * Instantiates a new geometry by circle judgement.
     *
     * @param considerBoundaryIntersection the consider boundary intersection
     */
    public GeometryByCircleJudgement(boolean considerBoundaryIntersection) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
    }

    @Override
    public Iterator<PairGeometry<Circle, T>> call(Iterator<T> iteratorObject, Iterator<Circle> iteratorWindow) throws Exception {
        List<PairGeometry<Circle, T>> result = new ArrayList<>();
        List<T> queryObjects = new ArrayList<>();
        while(iteratorObject.hasNext())
        {
            queryObjects.add(iteratorObject.next());
        }
        while (iteratorWindow.hasNext()) {
            Circle window = (Circle) iteratorWindow.next();
            HashSet<Geometry> resultHashSet = new HashSet<Geometry>();
            for (int i =0;i<queryObjects.size();i++) {
                Geometry object = (Geometry) queryObjects.get(i);
                if (considerBoundaryIntersection) {
                    if (window.intersects(object)) {
                        resultHashSet.add(object);
                    }
                } else {
                    if (window.covers(object)) {
                        resultHashSet.add(object);
                    }
                }
            }
            if (resultHashSet.size() == 0) continue;
            result.add(new PairGeometry(window,resultHashSet));
        }
        return result.iterator();
    }
}
