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
package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

object OssLikeDataSource {
  def checkAndGlobPathIfNecessary(
      paths: Seq[String],
      conf: Configuration,
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean,
      numThreads: Integer,
      enableGlobbing: Boolean): Seq[Path] = Seq.empty
}

// Mimics a distribution that adds an extra overload next to the standard one (e.g. Dataproc)
object ExtraOverloadDataSource {
  def checkAndGlobPathIfNecessary(
      paths: Seq[String],
      conf: Configuration,
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean,
      numThreads: Integer,
      enableGlobbing: Boolean): Seq[Path] = Seq.empty

  def checkAndGlobPathIfNecessary(
      paths: Seq[String],
      conf: Configuration,
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean,
      numThreads: Integer,
      enableGlobbing: Boolean,
      extraFlag: Boolean,
      extraOption: Option[String]): Seq[Path] = Seq.empty
}

object DatabricksLikeDataSource {
  def checkAndGlobPathIfNecessary(
      paths: Seq[String],
      conf: Configuration,
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean,
      enableGlobbing: Boolean): Seq[Path] = Seq.empty
}

object UnsupportedDataSource {
  def checkAndGlobPathIfNecessary(paths: Seq[String], conf: Configuration): Seq[Path] =
    Seq.empty
}

class SedonaFileIndexHelperTest extends AnyFunSpec with Matchers {

  private def candidates(target: AnyRef) =
    target.getClass.getMethods.toSeq.filter(_.getName == "checkAndGlobPathIfNecessary")

  describe("SedonaFileIndexHelper.resolveCheckAndGlobMethod") {
    it("selects the standard overload regardless of the order of the candidates") {
      val all = candidates(ExtraOverloadDataSource)
      all.map(_.getParameterCount).sorted shouldBe Seq(6, 8)

      for (ordered <- Seq(all, all.reverse)) {
        val method = SedonaFileIndexHelper.resolveCheckAndGlobMethod(ordered)
        method.getParameterCount shouldBe 6
      }
    }

    it("selects the only standard overload") {
      SedonaFileIndexHelper
        .resolveCheckAndGlobMethod(candidates(OssLikeDataSource))
        .getParameterCount shouldBe 6
    }

    it("selects the Databricks overload without numThreads") {
      SedonaFileIndexHelper
        .resolveCheckAndGlobMethod(candidates(DatabricksLikeDataSource))
        .getParameterCount shouldBe 5
    }

    it("fails with the list of candidates when no overload is supported") {
      val e = intercept[NoSuchMethodException] {
        SedonaFileIndexHelper.resolveCheckAndGlobMethod(candidates(UnsupportedDataSource))
      }
      e.getMessage should include("checkAndGlobPathIfNecessary")
      e.getMessage should include("Candidates")
    }
  }
}
