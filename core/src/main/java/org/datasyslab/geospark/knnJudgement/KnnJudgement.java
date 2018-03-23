/*
 * FILE: KnnJudgement
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
package org.datasyslab.geospark.knnJudgement;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

// TODO: Auto-generated Javadoc

/**
 * The Class GeometryKnnJudgement.
 */
public class KnnJudgement<U extends Geometry, T extends Geometry>
        implements FlatMapFunction<Iterator<T>, T>, Serializable
{

    /**
     * The k.
     */
    int k;

    /**
     * The query center.
     */
    U queryCenter;

    /**
     * Instantiates a new geometry knn judgement.
     *
     * @param queryCenter the query center
     * @param k the k
     */
    public KnnJudgement(U queryCenter, int k)
    {
        this.queryCenter = queryCenter;
        this.k = k;
    }

    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    @Override
    public Iterator<T> call(Iterator<T> input)
            throws Exception
    {
        PriorityQueue<T> pq = new PriorityQueue<T>(k, new GeometryDistanceComparator(queryCenter, false));
        while (input.hasNext()) {
            if (pq.size() < k) {
                pq.offer(input.next());
            }
            else {
                T curpoint = input.next();
                double distance = curpoint.distance(queryCenter);
                double largestDistanceInPriQueue = ((Geometry) pq.peek()).distance(queryCenter);
                if (largestDistanceInPriQueue > distance) {
                    pq.poll();
                    pq.offer(curpoint);
                }
            }
        }
        ArrayList<T> res = new ArrayList<T>();
        for (int i = 0; i < k; i++) {
            res.add(pq.poll());
        }
        return res.iterator();
    }
}
