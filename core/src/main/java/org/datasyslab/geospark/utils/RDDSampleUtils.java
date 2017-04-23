/**
 * FILE: RDDSampleUtils.java
 * PATH: org.datasyslab.geospark.utils.RDDSampleUtils.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
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
     * @throws Exception the exception
     */
    public static int getSampleNumbers(Integer numPartitions, long totalNumberOfRecords) throws Exception {
    	long sampleNumbers;
    	/*
    	 * If the input RDD is too small, Geospark will use the entire RDD instead of taking samples.
    	 */
    	if(totalNumberOfRecords>=1000)
    	{
    		sampleNumbers = totalNumberOfRecords / 100;
    	}
    	else
    	{
    		sampleNumbers = totalNumberOfRecords;
    	}
    	
		if(sampleNumbers > Integer.MAX_VALUE) {
			sampleNumbers = Integer.MAX_VALUE;
		}
        int result=(int)sampleNumbers;
        // Partition size is too big. Should throw exception for this.
        
        if(sampleNumbers < 2*numPartitions ) {
            throw new Exception("[RDDSampleUtils][getSampleNumbers] Too many RDD partitions. Please make this RDD's partitions less than "+sampleNumbers/2);
        }
        
        return result;

	}
}
