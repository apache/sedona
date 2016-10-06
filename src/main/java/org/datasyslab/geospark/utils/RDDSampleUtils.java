package org.datasyslab.geospark.utils;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

public class RDDSampleUtils {
    /**
     * Calculate the number of sampled records to build grid for this RDD.
     * @param numPartitions the sampled records will be the multiple of numberOfPartitions.
     * @param totalNumberOfRecords the number of records in the RDD
     * @return the number of sample records
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
