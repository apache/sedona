/*
 * FILE: DedupParams
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
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Contains information necessary to activate de-dup logic in sub-classes of {@link JudgementBase}.
 */
public final class DedupParams
        implements Serializable
{
    private final List<Envelope> partitionExtents;

    /**
     * @param partitionExtents A list of partition extents in such an order that
     * an index of an element in this list matches partition ID.
     */
    public DedupParams(List<Envelope> partitionExtents)
    {
        this.partitionExtents = Objects.requireNonNull(partitionExtents, "partitionExtents");
    }

    public List<Envelope> getPartitionExtents()
    {
        return partitionExtents;
    }
}
