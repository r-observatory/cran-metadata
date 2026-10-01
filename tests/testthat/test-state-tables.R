# The manifest's state_tables and the refusal to publish lost state.
if (!exists("state_tables_shrunk", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

test_that("a missing db is a cold start, and an error when a prior release exists", {
  expect_equal(startup_state(TRUE, TRUE, FALSE), list(cold_start = FALSE, error = NULL))
  expect_equal(startup_state(FALSE, FALSE, FALSE), list(cold_start = TRUE, error = NULL))
  expect_equal(startup_state(FALSE, TRUE, TRUE), list(cold_start = TRUE, error = NULL))
  expect_match(startup_state(FALSE, TRUE, FALSE)$error,
               "a prior release exists but no metadata.db was downloaded")
})

test_that("state_table_counts keeps only the state tables", {
  got <- state_table_counts(list(authors = 5L, check_status_history = 7L,
                                 cran_check_deadlines = 2L))
  expect_equal(got, list(check_status_history = 7L, cran_check_deadlines = 2L))
  expect_equal(state_table_counts(list(authors = 5L)), setNames(list(), character(0)))
})

test_that("state_tables_shrunk names each table below its prior count", {
  prior <- list(state_tables = list(check_status_history = 7L, cran_maintainer_bounces = 3L))
  expect_equal(state_tables_shrunk(list(check_status_history = 7L,
                                        cran_maintainer_bounces = 4L), prior), character(0))
  expect_equal(state_tables_shrunk(list(check_status_history = 6L), prior),
               c("check_status_history has 6 rows, the prior manifest listed 7",
                 "cran_maintainer_bounces has 0 rows, the prior manifest listed 3"))
  expect_equal(state_tables_shrunk(list(check_status_history = 1L), NULL), character(0))
})

test_that("the manifest carries state_tables as an object and the cold_start flag", {
  db <- withr::local_tempfile(fileext = ".db")
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  DBI::dbExecute(con, "CREATE TABLE check_status_history (id INTEGER)")
  DBI::dbExecute(con, "INSERT INTO check_status_history VALUES (1), (2)")
  DBI::dbExecute(con, "CREATE TABLE authors (id INTEGER)")
  DBI::dbDisconnect(con)
  core <- summary_integrity_core(db, complete = FALSE)
  core$state_tables <- state_table_counts(core$tables)
  core$cold_start <- TRUE
  path <- withr::local_tempfile(fileext = ".json")
  write_manifest(path, core, generated_at = "2026-10-01T06:00:00Z")
  parsed <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  expect_equal(parsed$state_tables, list(check_status_history = 2L))
  expect_true(parsed$cold_start)
  expect_equal(prior_state_listing(parsed), c(check_status_history = 2L))
})
