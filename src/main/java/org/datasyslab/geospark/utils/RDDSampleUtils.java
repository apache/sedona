package org.datasyslab.geospark.utils;

/**
 * Created by jinxuanwu on 1/4/16.
 */
public class RDDSampleUtils {
    /**
     * Calculate the number of sampled records to build grid for this RDD.
     * @param numPartitions the sampled records will be the multiple of numberOfPartitions.
     * @param totalNumberOfRecords
     * @return
     */
    public static int getSampleNumbers(Integer numPartitions, long totalNumberOfRecords) {
		long sampleNumbers = totalNumberOfRecords / 100;
		if(sampleNumbers > Integer.MAX_VALUE) {
			sampleNumbers = Integer.MAX_VALUE;
		}
        int result;
        // Partition size is too big. Should throw exception for this.
        if(totalNumberOfRecords <= numPartitions ) {
            return -1;
        }

        Integer SquareOfnumPartitions = numPartitions * numPartitions;
        if (sampleNumbers < SquareOfnumPartitions) {
            result = 0;
        }
        else {
            result = (int) (sampleNumbers) / SquareOfnumPartitions * SquareOfnumPartitions;
        }
        return result;
	}
}
