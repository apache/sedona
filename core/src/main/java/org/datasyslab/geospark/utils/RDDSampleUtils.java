/*
 * FILE: RDDSampleUtils
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