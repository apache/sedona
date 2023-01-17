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
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

/**
 * Base class for partition level join implementations.
 * <p>
 * Provides `match` method to test whether a given pair of geometries satisfies join condition.
 * <p>
 */
abstract class JudgementBase
        implements Serializable
{

    private final SpatialPredicate spatialPredicate;
    private transient SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;

    /**
     * @param spatialPredicate spatial predicate as join condition
     */
    protected JudgementBase(SpatialPredicate spatialPredicate)
    {
        this.spatialPredicate = spatialPredicate;
    }

    /**
     * Looks up the extent of the current partition. If found, `match` method will
     * activate the logic to avoid emitting duplicate join results from multiple partitions.
     * <p>
     * Must be called before processing a partition. Must be called from the
     * same instance that will be used to process the partition.
     */
    protected void initPartition()
    {
        evaluator = SpatialPredicateEvaluators.create(spatialPredicate);
    }

    public boolean match(Geometry left, Geometry right)
    {
        return evaluator.eval(left, right);
    }
}
