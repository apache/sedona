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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Geometry;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class NestedLoopJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<U, T>>, Serializable
{
    private static final Logger log = LogManager.getLogger(NestedLoopJudgement.class);

    /**
     * @see JudgementBase
     */
    public NestedLoopJudgement(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams)
    {
        super(considerBoundaryIntersection, dedupParams);
    }

    @Override
    public Iterator<Pair<U, T>> call(Iterator<T> iteratorObject, Iterator<U> iteratorWindow)
            throws Exception
    {
        if (!iteratorObject.hasNext() || !iteratorWindow.hasNext()) {
            return Collections.emptyIterator();
        }

        initPartition();

        List<Pair<U, T>> result = new ArrayList<>();
        List<T> queryObjects = new ArrayList<>();
        while (iteratorObject.hasNext()) {
            queryObjects.add(iteratorObject.next());
        }
        while (iteratorWindow.hasNext()) {
            U window = iteratorWindow.next();
            for (int i = 0; i < queryObjects.size(); i++) {
                T object = queryObjects.get(i);
                //log.warn("Check "+window.toText()+" with "+object.toText());
                if (match(window, object)) {
                    result.add(Pair.of(window, object));
                }
            }
        }
        return result.iterator();
    }
}
