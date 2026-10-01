# validate_prior_db and prior_db_action: the prior metadata.db fails closed.
if (!exists("validate_prior_db", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

.state_db <- function(path, history_rows = 3L, deadlines = TRUE) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(con, "CREATE TABLE check_status_history (id INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL, status TEXT NOT NULL, flavor_summary TEXT, details TEXT,
    detected_at TEXT NOT NULL)")
  if (history_rows > 0) DBI::dbWriteTable(con, "check_status_history", data.frame(
    package = paste0("p", seq_len(history_rows)), status = "OK", flavor_summary = "{}",
    details = "[]", detected_at = "2026-09-30T06:00:00Z", stringsAsFactors = FALSE), append = TRUE)
  if (deadlines) {
    DBI::dbExecute(con, "CREATE TABLE cran_check_deadlines (package TEXT, episode_seq INTEGER)")
    DBI::dbExecute(con, "INSERT INTO cran_check_deadlines VALUES ('p1', 1), ('p2', 1)")
  }
  DBI::dbExecute(con, "CREATE TABLE cran_check_results (package TEXT, flavor TEXT)")
  DBI::dbDisconnect(con)
  invisible(path)
}

test_that("a clean db with a matching manifest is kept", {
  db <- .state_db(withr::local_tempfile(fileext = ".db"))
  res <- validate_prior_db(db, list(state_tables = list(check_status_history = 3L,
                                                        cran_check_deadlines = 2L)))
  expect_true(res$ok)
  expect_equal(res$counts[["check_status_history"]], 3L)
  expect_equal(res$counts[["cran_check_deadlines"]], 2L)
  expect_equal(prior_db_action(res$ok), "keep")
})

test_that("an unreadable file fails", {
  db <- withr::local_tempfile(fileext = ".db")
  writeBin(as.raw(c(0x53, 0x51, 0x4c, rep(0x00, 5000))), db)
  res <- validate_prior_db(db, NULL)
  expect_false(res$ok)
  expect_gt(length(res$problems), 0)
  expect_equal(prior_db_action(res$ok), "fail")
})

test_that("a table the manifest lists but the db lacks fails", {
  db <- .state_db(withr::local_tempfile(fileext = ".db"), deadlines = FALSE)
  res <- validate_prior_db(db, list(state_tables = list(check_status_history = 3L,
                                                        cran_check_deadlines = 2L)))
  expect_false(res$ok)
  expect_match(res$problems, "cran_check_deadlines is listed with 2 rows but missing", all = FALSE)
})

test_that("a state table with fewer rows than listed fails", {
  db <- .state_db(withr::local_tempfile(fileext = ".db"), history_rows = 2L)
  res <- validate_prior_db(db, list(state_tables = list(check_status_history = 3L)))
  expect_false(res$ok)
  expect_match(res$problems, "check_status_history has 2 rows, the prior manifest listed 3",
               all = FALSE)
})

test_that("a first run with no manifest still opens and reads the db", {
  db <- .state_db(withr::local_tempfile(fileext = ".db"))
  res <- validate_prior_db(db, NULL)
  expect_true(res$ok)
  expect_setequal(names(res$counts), c("check_status_history", "cran_check_deadlines"))
})

test_that("a manifest from before state_tables still guards the state tables it counted", {
  db <- .state_db(withr::local_tempfile(fileext = ".db"), history_rows = 2L)
  old_manifest <- list(tables = list(authors = 10L, check_status_history = 3L,
                                     cran_check_deadlines = 2L, cran_check_results = 0L))
  res <- validate_prior_db(db, old_manifest)
  expect_false(res$ok)
  expect_match(res$problems, "check_status_history has 2 rows", all = FALSE)
  expect_equal(prior_state_listing(old_manifest),
               c(check_status_history = 3L, cran_check_deadlines = 2L))
})

test_that("start_fresh discards only a db that failed validation", {
  expect_equal(prior_db_action(TRUE, start_fresh = TRUE), "keep")
  expect_equal(prior_db_action(FALSE, start_fresh = TRUE), "discard")
  expect_equal(prior_db_action(FALSE, start_fresh = FALSE), "fail")
})

test_that("read_prior_manifest is NULL for no file and an error for a broken one", {
  expect_null(read_prior_manifest(file.path(tempdir(), "no-such-manifest.json")))
  bad <- withr::local_tempfile(fileext = ".json")
  writeLines("{ not json", bad)
  expect_error(read_prior_manifest(bad))
})
