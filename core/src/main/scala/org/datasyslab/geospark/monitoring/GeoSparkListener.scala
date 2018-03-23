/*
 * FILE: GeoSparkListener.scala
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

package org.datasyslab.geospark.monitoring

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}

import scala.collection.mutable

class GeoSparkListener extends SparkListener {

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
      accumulables.find { case (k, v) => v.name == Some("geospark.spatialjoin." + name) }
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
