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

package org.apache.sedona.core.spatialRddTool;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Objects;

public class StatCalculator
        implements Serializable
{
    private final Envelope boundary;
    private final long count;

    public StatCalculator(Envelope boundary, long count)
    {
        Objects.requireNonNull(boundary, "Boundary cannot be null");
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be > 0");
        }
        this.boundary = boundary;
        this.count = count;
    }

    public static StatCalculator combine(StatCalculator agg1, StatCalculator agg2)
            throws Exception
    {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new StatCalculator(
                StatCalculator.combine(agg1.boundary, agg2.boundary),
                agg1.count + agg2.count);
    }

    public static Envelope combine(Envelope agg1, Envelope agg2)
            throws Exception
    {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new Envelope(
                Math.min(agg1.getMinX(), agg2.getMinX()),
                Math.max(agg1.getMaxX(), agg2.getMaxX()),
                Math.min(agg1.getMinY(), agg2.getMinY()),
                Math.max(agg1.getMaxY(), agg2.getMaxY()));
    }

    public static Envelope add(Envelope agg, Geometry object)
            throws Exception
    {
        return combine(object.getEnvelopeInternal(), agg);
    }

    public static StatCalculator add(StatCalculator agg, Geometry object)
            throws Exception
    {
        return combine(new StatCalculator(object.getEnvelopeInternal(), 1), agg);
    }

    public Envelope getBoundary()
    {
        return boundary;
    }

    public long getCount()
    {
        return count;
    }
}
