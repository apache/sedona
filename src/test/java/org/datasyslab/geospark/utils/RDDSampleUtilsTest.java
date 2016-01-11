package org.datasyslab.geospark.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jinxuanwu on 1/10/16.
 */
public class RDDSampleUtilsTest {

    @Test
    public void testGetSampleNumbers() throws Exception {
        assertEquals(1, RDDSampleUtils.getSampleNumbers(2, 10));
        assertEquals(1, RDDSampleUtils.getSampleNumbers(2, 100));
        assertEquals(8, RDDSampleUtils.getSampleNumbers(2, 1010));
        assertEquals(8, RDDSampleUtils.getSampleNumbers(2, 1110));
        assertEquals(1, RDDSampleUtils.getSampleNumbers(5, 1000));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(5, 10000));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(5, 10001));
        assertEquals(1000, RDDSampleUtils.getSampleNumbers(5, 100011));
        assertEquals(972, RDDSampleUtils.getSampleNumbers(6, 100011));
    }
}