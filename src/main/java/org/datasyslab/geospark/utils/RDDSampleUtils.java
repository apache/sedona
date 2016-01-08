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
		long sampleNumbers = totalNumberOfRecords / 10;
		if(sampleNumbers > Integer.MAX_VALUE) {
			sampleNumbers = Integer.MAX_VALUE;
		}

		int result =  (int) (sampleNumbers) / numPartitions * numPartitions;
        if (sampleNumbers < numPartitions) {
            result = 0;
        }
        if(result == 0) {
            //todo: need second though, how to deal with smal partition
            return 1;
        } else {

             return result;
        }

	}
}
