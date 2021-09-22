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
        assertEquals(1, RDDSampleUtils.getSampleNumbers(1, 1, -1));
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
        assertFailure(2, 1, -1);
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