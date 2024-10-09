package org.apache.sedona.core

import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.PointRDD
import org.apache.sedona.core.utils.ResultUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object PartitionTest {
  var sc: SparkContext = null

  var dataInputLocation: String = null

  var clusterResultOutputLocation: String = null

  var KNNResultOutputLocation: String = null

  var circleResultOutPutLocation: String = null

  var partitionResultOutPutLocation: String = null

  var clusterEvaluationOutPutLocation: String = null

  var dataFileName: String = null

  var dataFileType: String = null

  var numPartitions: Int = 0

  var realNumPartitions: Int = 0

  var useIndex: Boolean = true

  var indexType: IndexType = null

  var gridType: GridType = null

  var k: Int = 31

  var dcmRatio: Double = 0.9

  def main(args: Array[String]): Unit = {
    initailize()
    //    val pointRDD = readData()
    //    partition(pointRDD)
    //    allKnnJoin(pointRDD)
    sc.stop()
  }

  def initailize(): Unit = {
    val resourceFolder = System.getProperty("user.dir") + "/src/test/resources"
//    val resourceFolder = ""

    // 初始化环境
    val conf = new SparkConf().setAppName("QGCDC_Sedona")
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    //          conf.setMaster("spark://Master:7077")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.max", "512M")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .set("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
      //      .set("spark.driver.memory", "8G")
      //      .set("spark.executor.memory", "4G")
      .set("spark.executor.instances", "8")
      .set("spark.executor.cores", "8")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)
    val sparkSession = spark.getOrCreate()
    sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    indexType = IndexType.RTREE
    //    gridType = NewGridType.QUADTREE
    //    gridType = NewGridType.QUADTREEWF
    val sparkExecCores: Integer = conf.get("spark.executor.cores").toInt
    val sparkNumExec: Integer = conf.get("spark.executor.instances").toInt
    //    val sparkNumExec: Int = sc.getExecutorMemoryStatus.size
    System.out.println("sparkNumExec: " + sparkNumExec)
    val countAllCores: Int = sparkExecCores * sparkNumExec
    numPartitions = 16
    //    numPartitions = countAllCores
    dataFileType = "TXT1"
    dataFileType match {
      case "TXT1" => {
        //        for (i <- Range(1, 7)) {
        for (i <- List(1, 2)) {
          //          var dataFileName = "NDS" + i
          dataFileName = "DS" + i
          for (gType <- List(GridType.QUADTREE, GridType.ImprovedQT,GridType.QUADTREE_RTREE, GridType.KDBTREE, GridType.Voronoi, GridType.STRTREE,GridType.Hilbert)) {
            gridType = gType
            //          for (m <- Range(5, 31, 2)) {
            //            for (m <- Range(5, 61, 5)) {
            //            k = m
            k = 21
            //            for (n <- Range(50, 96, 5)) {
            dcmRatio = 70 / 100.0
            //              dcmRatio = n / 100.0
            println("文件名:" + dataFileName + "\t分区数:" + numPartitions + "\t分区类型:" + gridType + "\t邻居数:" + k + "\tDCM阈值比例:" + dcmRatio)
            //  val resourceFolder: String = "file:///vda/cdc"
            val filePrefix = "/" + dataFileName + "_" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio
            val formatter = DateTimeFormat.forPattern("MMdd")
            val time = formatter.print(LocalDate.now())
            println("time" + time)
            dataInputLocation = resourceFolder + "/data/SyntheticDatasets/" + dataFileName + "_new.csv"
            clusterResultOutputLocation = resourceFolder + "/ResultData/clusterResult/QGCDC/" + time + filePrefix + ".txt"
            KNNResultOutputLocation = resourceFolder + "/ResultData/KNNResult/QGCDC/" + time + filePrefix + ".txt"
            partitionResultOutPutLocation = resourceFolder + "/ResultData/partitionResult/QGCDC/" + time + filePrefix
            //                val inputResourceFolder: String = "hdfs://Master:9000/data/CDC"
            //                val outputResourceFolder: String = "/spark/CDC/result"
            //                dataInputLocation = inputResourceFolder + "/SyntheticDatasets/" + dataFileName + ".csv"
            //                clusterResultOutputLocation = outputResourceFolder + "/clusterResult/HKGCDC_Sedona/testRangeNDS/" + filePrefix+ ".txt"
            //                clusterEvaluationOutPutLocation = outputResourceFolder + "/clusterEvaluation/HKGCDC_Sedona/testRangeNDS.txt"
            val pointRDD = readData()
            partition(pointRDD)
            //            allKnnJoin(pointRDD)
          }
        }
        //        }
      }
      case "TXT2" => {
        for (i <- List("Levine", "Samusik")) {
          var dataFileName = i + "_UMAP"
          for (m <- Range(30, 71, 10)) {
            var k = m
            for (n <- Range(50, 96, 5)) {
              var dcmRatio = n / 100.0
              println("文件名:" + dataFileName + "\t分区数:" + numPartitions + "\t分区类型:" + gridType + "\t邻居数:" + k + "\tDCM阈值比例:" + dcmRatio)
              //                val resourceFolder: String = System.getProperty("user.dir")
              //                val dataInputLocation = resourceFolder + "/data/SyntheticDatasets/" + dataFileName + ".txt"
              //                val clusterResultOutputLocation = resourceFolder + "/ResultData/clusterResult/HKGCDC_Sedona/testS10912/" + dataFileName + "/" + dataFileName + "_" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio + ".txt"
              //                //                val KNNResultOutputLocation = resourceFolder + "/ResultData/KNNResult/HKGCDC_Sedona/testS10912/" + dataFileName + "/" + dataFileName + "_" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio + ".txt"
              //                val clusterEvaluationOutPutLocation = resourceFolder + "/ResultData/clusterEvaluation/HKGCDC_Sedona/testS10912.txt"
              val inputResourceFolder: String = "hdfs://Master:9000/data/CDC"
              val outputResourceFolder: String = "/spark/CDC/result"
              dataInputLocation = inputResourceFolder + "/SyntheticDatasets/" + dataFileName + ".csv"
              clusterResultOutputLocation = outputResourceFolder + "/clusterResult/HKGCDC_Sedona/testRange2/" + dataFileName + "/" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio + ".txt"
              clusterEvaluationOutPutLocation = outputResourceFolder + "/clusterEvaluation/HKGCDC_Sedona/testRange2.txt"

            }
          }
        }
      }
      case "CSV" => {
        //        for (i <- List(10000, 20000, 50000, 100000, 200000, 300000)) {
        for (i <- List(300000)) {
          var dataFileName = "hubei-" + i
          //          for (m <- Range((math.ceil(math.log(i) / math.log(2)) + 10).toInt, 5 * (math.ceil(math.log(i) / math.log(2))).toInt + 1, 5)) {
          //            var k = m
          var k = (math.ceil(math.log(i) / math.log(2)) + 10).toInt
          //              var k=5*(math.ceil(math.log(i)/math.log(2))).toInt
          //            for (n <- Range(70, 96, 5)) {
          //              var dcmRatio = n / 100.0
          var dcmRatio = 70 / 100.0
          println("文件名:" + dataFileName + "\t分区数:" + numPartitions + "\t分区类型:" + gridType + "\t邻居数:" + k + "\tDCM阈值比例:" + dcmRatio)
          val filePrefix = "/" + dataFileName + "_" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio
          val formatter = DateTimeFormat.forPattern("MMdd")
          val time = formatter.print(LocalDate.now())
          println("time" + time)
          dataInputLocation = resourceFolder + "/data/GeoDataSets/0.37 Million Enterprise Registration Data in Hubei Province/Points/" + dataFileName + "-new2.csv"
          clusterResultOutputLocation = resourceFolder + "/ResultData/clusterResult/QGCDC/" + time + filePrefix + ".txt"
          KNNResultOutputLocation = resourceFolder + "/ResultData/KNNResult/QGCDC/" + time + filePrefix + ".txt"
          partitionResultOutPutLocation = resourceFolder + "/ResultData/partitionResult/QGCDC/" + time + filePrefix
          //                        val inputResourceFolder: String = "hdfs://Master:9000/data/"
          //                        val outputResourceFolder: String = "/spark/CDC/result"
          //                        dataInputLocation = inputResourceFolder + dataFileName + "-new2.csv"
          //                        clusterResultOutputLocation = outputResourceFolder + "/clusterResult/QGCDC_Sedona/test0114/" + dataFileName + "/" + gridType + "_" + indexType + "_" + numPartitions + "_" + k + "_" + dcmRatio + ".txt"
          //                        clusterEvaluationOutPutLocation = outputResourceFolder + "/clusterEvaluation/QGCDC_Sedona/test0114.txt"
        }
      }
      //        }
      //      }
    }
  }

  def readData() = {
    //read data
    println("读取数据开始----------")
    val readDataStart: Long = System.currentTimeMillis()
    val spatialRDD: PointRDD = new PointRDD(sc, dataInputLocation, 0, FileDataSplitter.CSV, true, numPartitions)
    val readDataEnd: Long = System.currentTimeMillis()
    println("读取数据结束----------")
    println("时间：" + (readDataEnd - readDataStart) / 1000.0 + "秒")
    spatialRDD
  }

  def partition(spatialRDD: PointRDD) = {
    //partition data
    println("空间分区开始----------")
    val partitionStart: Long = System.currentTimeMillis()
    spatialRDD.setNeighborSampleNumber(k)
    spatialRDD.spatialPartitioning(gridType, numPartitions,false)
    val partitionEnd: Long = System.currentTimeMillis()
    println("空间分区结束----------")
    println("时间：" + (partitionEnd - partitionStart) / 1000.0 + "秒")
    ResultUtils.outputPartitionAndGrid(spatialRDD, partitionResultOutPutLocation + "_partition.csv", partitionResultOutPutLocation + "_grid.csv");
  }

  //  def allKnnJoin(spatialRDD: PointRDD) = {
  //    //all knn join query
  //    println("近邻搜索开始----------")
  //    val allKNNJoinStart: Long = System.currentTimeMillis()
  //    val knnRDD: RDD[(Point, util.List[PointDistance])] = AllKNNJoinQuery2.AllKNNJoinQueryWithDistance2(spatialRDD, k, useIndex).rdd
  //    knnRDD.collect()
  //    //    println("knnRDDPartitions: "+knnRDD.getNumPartitions)
  //    val allKNNJoinEnd: Long = System.currentTimeMillis()
  //    println("近邻搜索结束----------")
  //    println("时间：" + (allKNNJoinEnd - allKNNJoinStart) / 1000.0 + "秒")
  //    OutPutResult.outputSTRTreeNode(spatialRDD, partitionResultOutPutLocation + "_nodeBound.csv")
  //  }

}
