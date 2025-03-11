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
package org.apache.sedona.sql

import io.minio.{MinioClient, PutObjectArgs}
import org.apache.log4j.{Level, Logger}
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.testcontainers.containers.MinIOContainer

import java.io.FileInputStream

import java.util.concurrent.ThreadLocalRandom

trait TestBaseScala extends FunSpec with BeforeAndAfterAll {
  Logger.getRootLogger().setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.apache.sedona.core").setLevel(Level.WARN)

  val keyParserExtension = "spark.sedona.enableParserExtension"
  val warehouseLocation = System.getProperty("user.dir") + "/target/"
  val sparkSession = SedonaContext
    .builder()
    .master("local[*]")
    .appName("sedonasqlScalaTest")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    // We need to be explicit about broadcasting in tests.
    .config("sedona.join.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions")
    .config(keyParserExtension, ThreadLocalRandom.current().nextBoolean())
    .getOrCreate()

  val sparkSessionMinio = SedonaContext
    .builder()
    .master("local[*]")
    .appName("sedonasqlScalaTest")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0")
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("sedona.join.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()

  val resourceFolder = System.getProperty("user.dir") + "/../common/src/test/resources/"

  override def beforeAll(): Unit = {
    SedonaContext.create(sparkSession)
  }

  override def afterAll(): Unit = {
    // SedonaSQLRegistrator.dropAll(spark)
    // spark.stop
  }

  def loadCsv(path: String): DataFrame = {
    sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(path)
  }

  def withConf[T](conf: Map[String, String])(f: => T): T = {
    val oldConf = conf.keys.map(key => key -> sparkSession.conf.getOption(key))
    conf.foreach { case (key, value) => sparkSession.conf.set(key, value) }
    try {
      f
    } finally {
      oldConf.foreach { case (key, value) =>
        value match {
          case Some(v) => sparkSession.conf.set(key, v)
          case None => sparkSession.conf.unset(key)
        }
      }
    }
  }

  def putFileIntoBucket(
      bucketName: String,
      key: String,
      stream: FileInputStream,
      client: MinioClient): Unit = {
    val objectArguments = PutObjectArgs
      .builder()
      .bucket(bucketName)
      .`object`(key)
      .stream(stream, stream.available(), -1)
      .build()

    client.putObject(objectArguments)
  }

  def createMinioClient(container: MinIOContainer): MinioClient = {
    MinioClient
      .builder()
      .endpoint(container.getS3URL)
      .credentials(container.getUserName, container.getPassword)
      .build()
  }

  def adjustSparkSession(sparkSession: SparkSession, container: MinIOContainer): Unit = {
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", container.getS3URL)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", container.getUserName)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", container.getPassword)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", "2000")

    sparkSession.sparkContext.hadoopConfiguration.set("spark.sql.debug.maxToStringFields", "100")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  }
}
