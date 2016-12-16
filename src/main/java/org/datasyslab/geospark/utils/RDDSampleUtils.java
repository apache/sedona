/**
 * FILE: RDDSampleUtils.java
 * PATH: org.datasyslab.geospark.utils.RDDSampleUtils.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

// TODO: Auto-generated Javadoc
/**
 * The Class RDDSampleUtils.
 */

public class RDDSampleUtils {
    
    /**
     * Gets the sample numbers.
     *
     * @param numPartitions the num partitions
     * @param totalNumberOfRecords the total number of records
     * @return the sample numbers
     */
    public static int getSampleNumbers(Integer numPartitions, long totalNumberOfRecords) {
		long sampleNumbers = totalNumberOfRecords / 100;
		if(sampleNumbers > Integer.MAX_VALUE) {
			sampleNumbers = Integer.MAX_VALUE;
		}
		
        int result=(int)sampleNumbers;
        // Partition size is too big. Should throw exception for this.
        if(totalNumberOfRecords <= numPartitions ) {
            return -1;
        }
        if(sampleNumbers<numPartitions)
        {
        	return 0;
        }
        
        /*
        Integer SquareOfnumPartitions = numPartitions * numPartitions;
        if (sampleNumbers < SquareOfnumPartitions) {
            result = 0;
        }
        else {
        	//result=(int)sampleNumbers;
            result = (int) (sampleNumbers) / SquareOfnumPartitions * SquareOfnumPartitions;
        }
        */
        return result;

	}
}
