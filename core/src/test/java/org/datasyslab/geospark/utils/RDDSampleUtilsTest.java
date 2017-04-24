/**
 * FILE: RDDSampleUtilsTest.java
 * PATH: org.datasyslab.geospark.utils.RDDSampleUtilsTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import static org.junit.Assert.assertEquals;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import org.junit.Test;

// TODO: Auto-generated Javadoc
/**
 * The Class RDDSampleUtilsTest.
 */
public class RDDSampleUtilsTest {

    /**
     * Test get sample numbers.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetSampleNumbers() throws Exception {
        assertEquals(10, RDDSampleUtils.getSampleNumbers(2, 10));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(2, 100));
        assertEquals(10, RDDSampleUtils.getSampleNumbers(5, 1000));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(5, 10000));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(5, 10001));
        assertEquals(1000, RDDSampleUtils.getSampleNumbers(5, 100011));
        assertEquals(1000, RDDSampleUtils.getSampleNumbers(6, 100011));
    }
    
    /**
     * Test too many partitions.
     *
     * @throws Exception the exception
     */
    @Test(expected=Exception.class)
    public void testTooManyPartitions() throws Exception
    {
        assertEquals(10, RDDSampleUtils.getSampleNumbers(6, 1010));
        assertEquals(11, RDDSampleUtils.getSampleNumbers(6, 1110));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(100, 10000));
    }
}