/*
 * FILE: LeftNestedLoopJudgement
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

import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction2;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LeftNestedLoopJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<U, T>>, Serializable
{
    private static final Logger log = LogManager.getLogger(LeftNestedLoopJudgement.class);
    private final boolean outer;
    private final T emptyElement;

    /**
     * @see JudgementBase
     */
    public LeftNestedLoopJudgement(boolean considerBoundaryIntersection,
                                   boolean swapLeftRight,
                                   @Nullable DedupParams dedupParams,
                                   boolean outer, T emptyElement)
    {
        super(considerBoundaryIntersection, swapLeftRight, dedupParams);
        this.outer = outer;
        this.emptyElement = emptyElement;
    }

    @Override
    public Iterator<Pair<U, T>> call(Iterator<T> iteratorObject, Iterator<U> iteratorWindow)
            throws Exception
    {
        initPartition();

        List<Pair<U, T>> result = new ArrayList<>();
        List<T> queryObjects = new ArrayList<>();
        while (iteratorObject.hasNext()) {
            queryObjects.add(iteratorObject.next());
        }
        while (iteratorWindow.hasNext()) {
            U window = iteratorWindow.next();
            boolean found = false;
            for (int i = 0; i < queryObjects.size(); i++) {
                T object = queryObjects.get(i);
                //log.warn("Check "+window.toText()+" with "+object.toText());
                if (match(window, object)) {
                    result.add(Pair.of(window, object));
                    found = true;
                }
            }
            if (outer && !found) {
                result.add(Pair.of(window, emptyElement));
            }
        }
        return result.iterator();
    }
}
