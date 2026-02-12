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

import org.apache.spark.SparkFiles
import org.apache.spark.sql.sedona_sql.expressions.LibPostalDataLoader
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class LibPostalDataLoaderTest extends TestBaseScala with Matchers {

  describe("LibPostalDataLoader") {

    describe("isRemotePath") {
      it("should return false for local paths") {
        LibPostalDataLoader.isRemotePath("/tmp/libpostal/") shouldBe false
      }

      it("should return false for relative paths") {
        LibPostalDataLoader.isRemotePath("data/libpostal/") shouldBe false
      }

      it("should return false for file:// URIs") {
        LibPostalDataLoader.isRemotePath("file:///tmp/libpostal/") shouldBe false
      }

      it("should return true for hdfs:// URIs") {
        LibPostalDataLoader.isRemotePath("hdfs:///data/libpostal/") shouldBe true
      }

      it("should return true for hdfs:// URIs with host") {
        LibPostalDataLoader.isRemotePath("hdfs://namenode:9000/data/libpostal/") shouldBe true
      }

      it("should return true for s3a:// URIs") {
        LibPostalDataLoader.isRemotePath("s3a://my-bucket/libpostal/") shouldBe true
      }

      it("should return true for gs:// URIs") {
        LibPostalDataLoader.isRemotePath("gs://my-bucket/libpostal/") shouldBe true
      }

      it("should return true for abfs:// URIs") {
        LibPostalDataLoader.isRemotePath(
          "abfs://container@account.dfs.core.windows.net/libpostal/") shouldBe true
      }

      it("should return true for wasb:// URIs") {
        LibPostalDataLoader.isRemotePath(
          "wasb://container@account.blob.core.windows.net/libpostal/") shouldBe true
      }

      it("should return false for empty string") {
        LibPostalDataLoader.isRemotePath("") shouldBe false
      }

      it("should return false for Windows-like paths") {
        // Single-letter scheme like C: should not be treated as remote
        LibPostalDataLoader.isRemotePath("C:\\libpostal\\data\\") shouldBe false
      }
    }

    describe("resolveDataDir") {
      it("should return local path unchanged") {
        val tempDir = Files.createTempDirectory("sedona-libpostal-test").toFile
        try {
          val result = LibPostalDataLoader.resolveDataDir(tempDir.getAbsolutePath)
          result shouldBe tempDir.getAbsolutePath
        } finally {
          tempDir.delete()
        }
      }

      it("should normalize file: URI to plain local path") {
        val tempDir = Files.createTempDirectory("sedona-libpostal-test").toFile
        try {
          val fileUri = tempDir.toURI.toString
          val result = LibPostalDataLoader.resolveDataDir(fileUri)
          result should not startWith "file:"
          result shouldBe tempDir.getAbsolutePath
        } finally {
          tempDir.delete()
        }
      }

      it("should throw IllegalStateException when remote data not found in SparkFiles") {
        val remoteUri = "hdfs:///data/nonexistent-libpostal-data/"

        val exception = intercept[IllegalStateException] {
          LibPostalDataLoader.resolveDataDir(remoteUri)
        }
        exception.getMessage should include("not found via SparkFiles")
        exception.getMessage should include("sc.addFile")
        exception.getMessage should include("recursive = true")
      }

      // This test simulates the SparkFiles resolution path without actually calling
      // sc.addFile, which would permanently register the remote URI in SparkContext's
      // internal state and cause downstream test failures when the remote endpoint is
      // no longer available. Instead, we place mock data directly in SparkFiles root.
      it("should resolve remote path when data is present in SparkFiles directory") {
        val sparkFilesDir = new File(SparkFiles.getRootDirectory())
        val mockDataDir = new File(sparkFilesDir, "libpostal-sparkfiles-test")
        try {
          // Create mock libpostal data in the SparkFiles root directory
          val subdirs = Seq("address_parser", "language_classifier", "transliteration")
          for (subdir <- subdirs) {
            val subdirFile = new File(mockDataDir, subdir)
            subdirFile.mkdirs()
            Files.write(new File(subdirFile, "model.dat").toPath, s"data for $subdir".getBytes)
          }

          // resolveDataDir should find the data via SparkFiles.get(basename)
          val remotePath = "s3a://my-bucket/data/libpostal-sparkfiles-test"
          val localPath = LibPostalDataLoader.resolveDataDir(remotePath)

          // Verify the resolved path is local and contains all expected data
          localPath should not startWith "s3a://"
          val localDir = new File(localPath)
          localDir.exists() shouldBe true
          localDir.isDirectory shouldBe true
          localPath should endWith(File.separator)

          for (subdir <- subdirs) {
            val localSubdir = new File(localDir, subdir)
            localSubdir.exists() shouldBe true
            localSubdir.isDirectory shouldBe true
            new File(localSubdir, "model.dat").exists() shouldBe true
          }
        } finally {
          // Clean up the mock data directory
          if (mockDataDir.exists()) {
            mockDataDir.listFiles().foreach { sub =>
              sub.listFiles().foreach(_.delete())
              sub.delete()
            }
            mockDataDir.delete()
          }
        }
      }
    }
  }
}
