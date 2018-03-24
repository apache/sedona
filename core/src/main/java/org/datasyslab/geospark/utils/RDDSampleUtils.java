/**
 * FILE: RDDSampleUtils.java
 * PATH: org.datasyslab.geospark.utils.RDDSampleUtils.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

// TODO: Auto-generated Javadoc

/**
 * The Class RDDSampleUtils.
 */

public class RDDSampleUtils
{

    /**
     * Returns the number of samples to take to partition the RDD into specified number of partitions.
     * <p>
     * Number of partitions cannot exceed half the number of records in the RDD.
     * <p>
     * Returns total number of records if it is < 1000. Otherwise, returns 1% of the total number
     * of records or twice the number of partitions whichever is larger. Never returns a
     * number > Integer.MAX_VALUE.
     * <p>
     * If desired number of samples is not -1, returns that number.
     *
     * @param numPartitions the num partitions
     * @param totalNumberOfRecords the total number of records
     * @param givenSampleNumbers the given sample numbers
     * @return the sample numbers
     * @throws IllegalArgumentException if requested number of samples exceeds total number of records
     * or if requested number of partitions exceeds half of total number of records
     */
    public static int getSampleNumbers(int numPartitions, long totalNumberOfRecords, int givenSampleNumbers)
    {
        if (givenSampleNumbers > 0) {
            if (givenSampleNumbers > totalNumberOfRecords) {
                throw new IllegalArgumentException("[GeoSpark] Number of samples " + givenSampleNumbers + " cannot be larger than total records num " + totalNumberOfRecords);
            }
            return givenSampleNumbers;
        }

        // Make sure that number of records >= 2 * number of partitions
        if (totalNumberOfRecords < 2 * numPartitions) {
            throw new IllegalArgumentException("[GeoSpark] Number of partitions " + numPartitions + " cannot be larger than half of total records num " + totalNumberOfRecords);
        }

        if (totalNumberOfRecords < 1000) {
            return (int) totalNumberOfRecords;
        }

        final int minSampleCnt = numPartitions * 2;
        return (int) Math.max(minSampleCnt, Math.min(totalNumberOfRecords / 100, Integer.MAX_VALUE));
    }
}