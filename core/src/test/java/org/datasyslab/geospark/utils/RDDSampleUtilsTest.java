/**
 * FILE: RDDSampleUtilsTest.java
 * PATH: org.datasyslab.geospark.utils.RDDSampleUtilsTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
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