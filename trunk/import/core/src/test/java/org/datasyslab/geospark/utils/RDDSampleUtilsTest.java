/*
 * FILE: RDDSampleUtilsTest
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
package org.datasyslab.geospark.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RDDSampleUtilsTest
{

    /**
     * Test get sample numbers.
     */
    @Test
    public void testGetSampleNumbers()
    {
        assertEquals(10, RDDSampleUtils.getSampleNumbers(2, 10, -1));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(2, 100, -1));
        assertEquals(10, RDDSampleUtils.getSampleNumbers(5, 1000, -1));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(5, 10000, -1));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(5, 10001, -1));
        assertEquals(1000, RDDSampleUtils.getSampleNumbers(5, 100011, -1));
        assertEquals(99, RDDSampleUtils.getSampleNumbers(6, 100011, 99));
        assertEquals(999, RDDSampleUtils.getSampleNumbers(20, 999, -1));
        assertEquals(40, RDDSampleUtils.getSampleNumbers(20, 1000, -1));
    }

    /**
     * Test too many partitions.
     */
    @Test
    public void testTooManyPartitions()
    {
        assertFailure(505, 999);
        assertFailure(505, 1000);
        assertFailure(10, 1000, 2100);
    }

    private void assertFailure(int numPartitions, long totalNumberOfRecords)
    {
        assertFailure(numPartitions, totalNumberOfRecords, -1);
    }

    private void assertFailure(int numPartitions, long totalNumberOfRecords, int givenSampleNumber)
    {
        try {
            RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberOfRecords, givenSampleNumber);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            // expected
        }
    }
}