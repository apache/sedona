/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.utdallas.cg.spatial_rdd.core.approximation;

public class RddSamplingUtils {

  /**
   * Returns the number of samples to take to partition the RDD into specified number of partitions.
   *
   * <p>Number of partitions cannot exceed half the number of records in the RDD.
   *
   * <p>Returns total number of records if it is < 1000. Otherwise, returns 1% of the total number
   * of records or twice the number of partitions whichever is larger. Never returns a number >
   * Integer.MAX_VALUE.
   *
   * <p>If desired number of samples is not -1, returns that number.
   *
   * @param numPartitions the num partitions
   * @param totalNumberOfRecords the total number of records
   * @return the sample numbers
   */
  public static int getSampleNumbers(int numPartitions, long totalNumberOfRecords) {

    if (totalNumberOfRecords < 1000) {
      return (int) totalNumberOfRecords;
    }

    final long minSampleCnt = Math.min(numPartitions * 2L, totalNumberOfRecords);
    return (int) Math.max(minSampleCnt, Math.min(totalNumberOfRecords / 100, Integer.MAX_VALUE));
  }
}
