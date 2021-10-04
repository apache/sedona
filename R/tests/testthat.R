#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

library(sparklyr)
library(apache.sedona)
library(testthat)

# increase timeout for downloading Apache Spark tgz files
options(timeout = 600)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  on.exit({
    spark_disconnect_all()
  })

  filter <- Sys.getenv("TESTTHAT_FILTER", unset = "")
  if (identical(filter, "")) filter <- NULL

  reporter <- MultiReporter$new(reporters = list(
    ProgressReporter$new(show_praise = FALSE),
    CheckReporter$new(),
    SummaryReporter$new(show_praise = FALSE)
  ))
  test_check("apache.sedona", filter = filter, reporter = reporter)
}
