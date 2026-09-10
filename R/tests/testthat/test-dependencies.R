# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

context("spark_dependencies")

sedona_packages <- function(spark_version, scala_version = NULL) {
  old <- Sys.getenv("SEDONA_JAR_FILES", unset = NA)
  Sys.unsetenv("SEDONA_JAR_FILES")
  on.exit(if (!is.na(old)) Sys.setenv(SEDONA_JAR_FILES = old))
  apache.sedona:::spark_dependencies(numeric_version(spark_version), scala_version)$packages
}

test_that("Spark 3.x requests the Scala 2.12 artifact", {
  expect_true("org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1" %in% sedona_packages("3.5", "2.12"))
  expect_true("org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1" %in% sedona_packages("3.5"))
})

test_that("Spark 4.x requests the Scala 2.13 artifact even when sparklyr passes 2.12", {
  # sparklyr hands extensions "2.12" for every Spark >= 3.0
  expect_true("org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.9.1" %in% sedona_packages("4.0", "2.12"))
  expect_true("org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.1" %in% sedona_packages("4.1"))
  expect_false(any(grepl("_2.12:", sedona_packages("4.0", "2.12"))))
})

test_that("Spark 2.x is rejected", {
  expect_error(sedona_packages("2.4"), "Unsupported Spark version")
})

test_that("SEDONA_JAR_FILES replaces the Sedona coordinate with local jars", {
  old <- Sys.getenv("SEDONA_JAR_FILES", unset = NA)
  Sys.setenv(SEDONA_JAR_FILES = "/tmp/a.jar:/tmp/b.jar")
  on.exit(if (is.na(old)) Sys.unsetenv("SEDONA_JAR_FILES") else Sys.setenv(SEDONA_JAR_FILES = old))
  deps <- apache.sedona:::spark_dependencies(numeric_version("4.0"), "2.12")
  expect_equal(deps$jars, c("/tmp/a.jar", "/tmp/b.jar"))
  expect_false(any(grepl("sedona-spark-shaded", deps$packages)))
})
