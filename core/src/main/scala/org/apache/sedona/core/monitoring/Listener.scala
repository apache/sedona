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

package org.apache.sedona.core.monitoring

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}

import scala.collection.mutable

class Listener extends SparkListener {

  private val counterNames = Seq("buildCount", "streamCount", "candidateCount", "resultCount")

  private val taskCpuTime: mutable.Map[(Integer, Integer), Long] = mutable.Map()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.reason == org.apache.spark.Success) {
      val cpuTime = taskEnd.taskMetrics.executorCpuTime
      // Task ID is a string made of two integers separated with period:
      //  <partition-id>.<attempt-id>
      val partitionId = Integer.parseInt(taskEnd.taskInfo.id.split('.')(0))

      taskCpuTime((taskEnd.stageId, partitionId)) = cpuTime
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = {
    val accumulables = stageCompleted.stageInfo.accumulables

    def getCounterOption(name: String) = {
      accumulables.find { case (k, v) => v.name == Some("sedona.spatialjoin." + name) }
    }

    def getCounter(name: String) = {
      getCounterOption(name).get._2.value.get
    }

    if (getCounterOption("buildCount").isDefined) {

      val stageId = stageCompleted.stageInfo.stageId

      val buildCounts: Map[Int, Long] = getCounter("buildCount").asInstanceOf[Map[Int, Long]]
      val streamCounts: Map[Int, Long] = getCounter("streamCount").asInstanceOf[Map[Int, Long]]
      val candidateCounts: Map[Int, Long] = getCounter("candidateCount").asInstanceOf[Map[Int, Long]]
      val resultCounts: Map[Int, Long] = getCounter("resultCount").asInstanceOf[Map[Int, Long]]

      val stats: List[(Int, Long, Long, Long, Long, Long)] =
        buildCounts.map {
          case (partitionId, buildCount) => {
            val streamCount: Long = streamCounts.getOrElse(partitionId, -1)
            val candidateCount: Long = candidateCounts.getOrElse(partitionId, -1)
            val resultCount: Long = resultCounts.getOrElse(partitionId, -1)
            val cpuTime: Long = taskCpuTime.getOrElse((stageId, partitionId), -1)
            (partitionId, buildCount, streamCount, candidateCount, resultCount, cpuTime)
          }
        }.toList.sortBy {
          case (_, _, _, _, _, cpuTime) => cpuTime
        }

      Console.out.println("Spatial join is complete. Execution statistics:")
      Console.out.println("Partition\t CPU Time (s)\tBuild ##\tStream ##\tCandidates ##\tResults ##")
      stats.foreach {
        case (partitionId, buildCount, streamCount, candidateCount, resultCount, cpuTime) =>
          Console.out.println(f"$partitionId% 10d\t${cpuTime / 1000}% 10d" +
            f"$buildCount% 10d\t$streamCount% 10d\t$candidateCount% 10d\t$resultCount% 10d")
      }
    }
  }
}
