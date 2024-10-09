package org.apache.sedona.core.utils

import org.apache.sedona.core.spatialRDD.PointRDD
import org.apache.spark.TaskContext
import org.locationtech.jts.geom.{Envelope, Point}
import org.locationtech.jts.index.SpatialIndex
import org.locationtech.jts.index.strtree.STRtree

import java.io.{File, FileWriter}
import java.util

object ResultUtils {
  def outputPartitionAndGrid(spatialRDD: PointRDD,pPath: String,gPath:String): Unit ={
    IOUtils.createFile(pPath)
    IOUtils.createFile(gPath)
    val pointsList: util.List[(Int,Point)] = spatialRDD.spatialPartitionedRDD.map(point => (TaskContext.getPartitionId(),point)).collect()
    val gridsList: util.List[Envelope] = spatialRDD.getPartitioner.getGrids
    val numPartitions: Int = spatialRDD.getPartitioner.getPartition(2)
//    问题：空分区
//    val partitionSize: Array[(Int, Int)] = spatialRDD.spatialPartitionedRDD.map(point => (TaskContext.getPartitionId(), 1)).rdd
//      .groupBy(_._1) // 按分区 ID 进行分组
//      .mapValues(_.size)// 统计每个分区的元素个数
//      .collect()
    val partitionSize: Array[(Int, Int)] = spatialRDD.spatialPartitionedRDD.rdd.mapPartitions(iterator => {
      val partitionName = TaskContext.getPartitionId() // Get the name of the current partition
      var size = 0
      while (iterator.hasNext) {
        iterator.next()
        size += 1
      }
      Iterator((partitionName, size))
    }, preservesPartitioning = true).collect()

    val pwriter = new FileWriter(new File(pPath))
    pointsList.forEach {
      case (partitionId,point) =>
        val split= (point.getUserData.toString).split(",|\\s+|;")
        val pointId = split(1)
        val label=split(0)
//        val pointId = split(split.length - 3)
        val row = s"${point.getCentroid.getX},${point.getCentroid.getY},$label,$pointId,$partitionId"
        pwriter.write(row + "\n") // 写入 CSV 行
    }

    val gwriter = new FileWriter(new File(gPath))
    for (i <- 0 until gridsList.size()) {
        val grid: Envelope = gridsList.get(i)
        val psize: (Int, Int) = partitionSize(i)
        val row=s"${grid.getMinX},${grid.getMaxX},${grid.getMinY},${grid.getMaxY},${psize._1},${psize._2}"
        gwriter.write(row+"\n")
    }
    pwriter.close()
    gwriter.close()
  }

  def outputSTRTreeNode(spatialRDD: PointRDD,nPath: String): Unit ={
    IOUtils.createFile(nPath)
    val boundList: util.List[Envelope] = spatialRDD.indexedRDD.map((index:SpatialIndex)=>{
      val strIndex: STRtree =index.asInstanceOf[STRtree]
      strIndex.getRoot.getBounds.asInstanceOf[Envelope]
    }).collect()
    val nwriter: FileWriter = new FileWriter(new File(nPath))
    for (i <- 0 until boundList.size()) {
      val grid: Envelope = boundList.get(i)
      val row: String =s"${grid.getMinX},${grid.getMaxX},${grid.getMinY},${grid.getMaxY}"
      nwriter.write(row+"\n")
    }
    nwriter.close()
  }

}
