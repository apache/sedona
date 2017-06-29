/**
 * FILE: GeometryByPolygonJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgementUsingIndex.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.datasyslab.geospark.geometryObjects.PairGeometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class GeometryByPolygonJudgementUsingIndex.
 */
public class GeometryByPolygonJudgementUsingIndex implements FlatMapFunction2<Iterator<SpatialIndex>, Iterator<Object>, PairGeometry>, Serializable {

    /**
     * The consider boundary intersection.
     */
    boolean considerBoundaryIntersection = false;

    /**
     * Instantiates a new geometry by polygon judgement using index.
     *
     * @param considerBoundaryIntersection the consider boundary intersection
     */
    public GeometryByPolygonJudgementUsingIndex(boolean considerBoundaryIntersection) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
    }

    @Override
    public Iterator<PairGeometry> call(Iterator<SpatialIndex> iteratorTree, Iterator<Object> iteratorWindow) throws Exception {
        List<PairGeometry> result = new ArrayList<>();

        if (!iteratorTree.hasNext()) {
            return result.iterator();
        }
        SpatialIndex treeIndex = iteratorTree.next();
        if (treeIndex instanceof STRtree) {
            treeIndex = (STRtree) treeIndex;
        } else {
            treeIndex = (Quadtree) treeIndex;
        }
        while (iteratorWindow.hasNext()) {
            Polygon window = (Polygon) iteratorWindow.next();
            List<Geometry> queryResult = new ArrayList<Geometry>();
            queryResult = treeIndex.query(window.getEnvelopeInternal());
            if (queryResult.size() == 0) continue;
            HashSet<Geometry> objectHashSet = new HashSet<Geometry>();
            for (Geometry spatialObject : queryResult) {
                // Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
                if (considerBoundaryIntersection) {
                    if (window.intersects(spatialObject)) {
                        objectHashSet.add(spatialObject);
                    }
                } else {
                    if (window.covers(spatialObject)) {
                        objectHashSet.add(spatialObject);
                    }
                }
            }
            if (objectHashSet.size() == 0) continue;
            result.add(new PairGeometry(window, objectHashSet));
        }
        return result.iterator();
    }
}
