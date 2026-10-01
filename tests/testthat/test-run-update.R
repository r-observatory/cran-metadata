# scripts/update.R end to end, offline.

test_that("a first run with no prior release is a cold start and lists its state tables", {
  dir <- withr::local_tempdir()
  r <- .run_update(dir, .cran_fixture())
  expect_equal(r$status, 0L, info = paste(r$out, collapse = "\n"))
  m <- jsonlite::fromJSON(file.path(dir, "manifest.json"), simplifyVector = FALSE)
  expect_true(m$cold_start)
  expect_equal(m$state_tables$check_status_history, 2L)
  expect_true(any(grepl("Cold start", readLines(file.path(dir, "release_notes.md")))))
})

test_that("a prior release with no downloaded db stops before touching anything", {
  dir <- withr::local_tempdir()
  writeLines("v20261001-060000", file.path(dir, "prior-tag.txt"))
  r <- .run_update(dir, .cran_fixture())
  expect_false(r$status == 0L)
  expect_true(any(grepl("a prior release exists but no metadata.db was downloaded", r$out)))
  expect_false(file.exists(file.path(dir, "metadata.db")))
})

test_that("a discarded db declared with COLD_START runs as a cold start", {
  dir <- withr::local_tempdir()
  writeLines("v20261001-060000", file.path(dir, "prior-tag.txt"))
  r <- .run_update(dir, .cran_fixture(), env = "COLD_START=true")
  expect_equal(r$status, 0L, info = paste(r$out, collapse = "\n"))
  expect_true(jsonlite::fromJSON(file.path(dir, "manifest.json"))$cold_start)
})

test_that("state rows lost since the prior manifest stop the run before the manifest", {
  dir <- withr::local_tempdir()
  expect_equal(.run_update(dir, .cran_fixture())$status, 0L)
  file.remove(file.path(dir, "manifest.json"))
  writeLines("v20261001-060000", file.path(dir, "prior-tag.txt"))
  writeLines('{"state_tables": {"check_status_history": 99}}',
             file.path(dir, "prior-manifest.json"))
  r <- .run_update(dir, .cran_fixture())
  expect_false(r$status == 0L)
  expect_true(any(grepl("check_status_history has 2 rows, the prior manifest listed 99", r$out)))
  expect_false(file.exists(file.path(dir, "manifest.json")))
})

test_that("a second run carries state and is not a cold start", {
  dir <- withr::local_tempdir()
  expect_equal(.run_update(dir, .cran_fixture())$status, 0L)
  file.rename(file.path(dir, "manifest.json"), file.path(dir, "prior-manifest.json"))
  writeLines("v20261001-060000", file.path(dir, "prior-tag.txt"))
  r <- .run_update(dir, .cran_fixture(status = c(pkgA = "NOTE", pkgB = "ERROR")))
  expect_equal(r$status, 0L, info = paste(r$out, collapse = "\n"))
  m <- jsonlite::fromJSON(file.path(dir, "manifest.json"), simplifyVector = FALSE)
  expect_false(m$cold_start)
  expect_equal(m$state_tables$check_status_history, 3L)
})

test_that("a run stores the checked version and flags and names them in the details", {
  dir <- withr::local_tempdir()
  expect_equal(.run_update(dir, .cran_fixture(check_version = c(pkgA = "1.0", pkgB = "1.9")))$status, 0L)
  got <- .db_query(dir, "SELECT package, version, flags FROM cran_check_results ORDER BY package")
  expect_equal(got$version, c("1.0", "1.9"))
  expect_equal(got$flags, c(NA, "--no-vignettes"))
  details <- .db_query(dir, "SELECT details FROM check_status_history WHERE package = 'pkgB'")$details
  entry <- jsonlite::fromJSON(details)
  expect_equal(entry$version, "1.9")
  expect_equal(entry$flags, "--no-vignettes")
  expect_equal(entry$check_name, "tests")
})
