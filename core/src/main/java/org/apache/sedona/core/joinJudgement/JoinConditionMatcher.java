/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.joinJudgement;

import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

/**
 * Match geometries using join condition with a mechanism for deduplication.
 */
public abstract class JoinConditionMatcher implements SpatialPredicateEvaluators.SpatialPredicateEvaluator {

    public boolean match(Geometry left, Geometry right, HalfOpenRectangle extent) {
        if (extent != null) {
            // Handle easy case: points. Since each point is assigned to exactly one partition,
            // different partitions cannot emit duplicate results.
            if (left instanceof Point || right instanceof Point) {
                return eval(left, right);
            }

            // Neither geometry is a point

            // Check if reference point of the intersection of the bounding boxes lies within
            // the extent of this partition. If not, don't run any checks. Let the partition
            // that contains the reference point do all the work.
            Envelope intersection =
                    left.getEnvelopeInternal().intersection(right.getEnvelopeInternal());
            if (!intersection.isNull()) {
                final Point referencePoint =
                        makePoint(intersection.getMinX(), intersection.getMinY(), left.getFactory());
                if (!extent.contains(referencePoint)) {
                    return false;
                }
            }
        }

        return eval(left, right);
    }

    private static Point makePoint(double x, double y, GeometryFactory factory) {
        return factory.createPoint(new Coordinate(x, y));
    }

    public static JoinConditionMatcher create(SpatialPredicate predicate) {
        switch (predicate) {
            case CONTAINS:
                return new ContainsMatcher();
            case INTERSECTS:
                return new IntersectsMatcher();
            case WITHIN:
                return new WithinMatcher();
            case COVERS:
                return new CoversMatcher();
            case COVERED_BY:
                return new CoveredByMatcher();
            case TOUCHES:
                return new TouchesMatcher();
            case OVERLAPS:
                return new OverlapsMatcher();
            case CROSSES:
                return new CrossesMatcher();
            case EQUALS:
                return new EqualsMatcher();
            default:
                throw new IllegalArgumentException("Invalid spatial predicate: " + predicate);
        }
    }

    private static class ContainsMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.ContainsEvaluator {}

    private static class IntersectsMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.IntersectsEvaluator {}

    private static class WithinMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.WithinEvaluator {}

    private static class CoversMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.CoversEvaluator {}

    private static class CoveredByMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.CoveredByEvaluator {}

    private static class TouchesMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.TouchesEvaluator {}

    private static class OverlapsMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.OverlapsEvaluator {}

    private static class CrossesMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.CrossesEvaluator {}

    private static class EqualsMatcher extends JoinConditionMatcher
            implements SpatialPredicateEvaluators.EqualsEvaluator {}
}
