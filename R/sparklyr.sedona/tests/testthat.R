library(sparklyr)
library(sparklyr.sedona)
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
  test_check("sparklyr.sedona", filter = filter, reporter = reporter)
}
