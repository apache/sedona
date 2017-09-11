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

public class RDDSampleUtils {
   
    
    /**
     * Gets the sample numbers.
     *
     * @param numPartitions the num partitions
     * @param totalNumberOfRecords the total number of records
     * @param givenSampleNumbers the given sample numbers
     * @return the sample numbers
     * @throws Exception the exception
     */
    public static int getSampleNumbers(Integer numPartitions, long totalNumberOfRecords, long givenSampleNumbers) throws Exception{
    	Long sampleNumber = new Long(0);

    	if(givenSampleNumbers>0)
    	{
    		// This means that the user manually specifies the sample number
    		sampleNumber = givenSampleNumbers;
    		return sampleNumber.intValue();
    	}
    	else
    	{
    		// Follow GeoSpark internal sampling rule
        	/*
        	 * If the input RDD is too small, Geospark will use the entire RDD instead of taking samples.
        	 */
        	if(totalNumberOfRecords>=1000)
        	{
        		sampleNumber = totalNumberOfRecords / 100;
        	}
        	else
        	{
        		sampleNumber = totalNumberOfRecords;
        	}
        	
    		if(sampleNumber > Integer.MAX_VALUE) {
    			sampleNumber = new Long(Integer.MAX_VALUE);
    		}
            if(sampleNumber < 2*numPartitions ) {
                // Partition size is too big. Should throw exception for this.
                throw new Exception("[RDDSampleUtils][getSampleNumbers] Too many RDD partitions. Call SpatialRDD.setSampleNumber() to manually increase sample or make partitionNum less than "+sampleNumber/2);
            }
            return sampleNumber.intValue();
    	}

	}
}