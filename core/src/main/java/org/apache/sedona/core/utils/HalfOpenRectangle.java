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

package org.apache.sedona.core.utils;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

public class HalfOpenRectangle
{
    private final Envelope envelope;

    public HalfOpenRectangle(Envelope envelope)
    {
        this.envelope = envelope;
    }

    public boolean contains(Point point)
    {
        return contains(point.getX(), point.getY());
    }

    public boolean contains(double x, double y)
    {
        return x >= envelope.getMinX() && x < envelope.getMaxX()
                && y >= envelope.getMinY() && y < envelope.getMaxY();
    }

    public Envelope getEnvelope()
    {
        return envelope;
    }
}
