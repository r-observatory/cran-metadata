# scripts/validate_prior.R as the workflow runs it.
.validate_script <- normalizePath(file.path("..", "..", "scripts", "validate_prior.R"))

.run_validate <- function(dir, start_fresh = "") {
  env_file <- file.path(dir, "github_env")
  file.create(env_file)
  out <- suppressWarnings(system2("Rscript", c(shQuote(.validate_script),
    shQuote(file.path(dir, "metadata.db")), shQuote(file.path(dir, "prior-manifest.json"))),
    env = c(paste0("START_FRESH=", start_fresh), paste0("GITHUB_ENV=", env_file)),
    stdout = TRUE, stderr = TRUE))
  list(status = if (is.null(attr(out, "status"))) 0L else attr(out, "status"),
       out = out, env = readLines(env_file))
}
.good_db <- function(dir) {
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "metadata.db"))
  DBI::dbExecute(con, "CREATE TABLE check_status_history (id INTEGER PRIMARY KEY,
    package TEXT, status TEXT, flavor_summary TEXT, details TEXT, detected_at TEXT)")
  DBI::dbExecute(con, "INSERT INTO check_status_history VALUES (1, 'a', 'OK', '{}', '[]', 'x')")
  DBI::dbDisconnect(con)
}
.bad_db <- function(dir) writeBin(as.raw(c(0x53, 0x51, 0x4c, rep(0x00, 5000))),
                                  file.path(dir, "metadata.db"))

test_that("a readable db passes and is kept", {
  dir <- withr::local_tempdir()
  .good_db(dir)
  writeLines('{"state_tables": {"check_status_history": 1}}', file.path(dir, "prior-manifest.json"))
  r <- .run_validate(dir)
  expect_equal(r$status, 0L)
  expect_true(file.exists(file.path(dir, "metadata.db")))
  expect_equal(r$env, character(0))
})

test_that("an unreadable db fails the step and stays on disk", {
  dir <- withr::local_tempdir()
  .bad_db(dir)
  r <- .run_validate(dir)
  expect_equal(r$status, 1L)
  expect_true(file.exists(file.path(dir, "metadata.db")))
  expect_true(any(grepl("^::error::", r$out)))
})

test_that("a shrunken state table fails the step", {
  dir <- withr::local_tempdir()
  .good_db(dir)
  writeLines('{"state_tables": {"check_status_history": 5}}', file.path(dir, "prior-manifest.json"))
  r <- .run_validate(dir)
  expect_equal(r$status, 1L)
  expect_true(any(grepl("check_status_history has 1 rows, the prior manifest listed 5", r$out)))
})

test_that("start_fresh removes an unreadable db and declares a cold start", {
  dir <- withr::local_tempdir()
  .bad_db(dir)
  writeLines('{"state_tables": {"check_status_history": 5}}', file.path(dir, "prior-manifest.json"))
  r <- .run_validate(dir, start_fresh = "true")
  expect_equal(r$status, 0L)
  expect_false(file.exists(file.path(dir, "metadata.db")))
  expect_false(file.exists(file.path(dir, "prior-manifest.json")))
  expect_equal(r$env, "COLD_START=true")
})

test_that("start_fresh keeps a db that passes", {
  dir <- withr::local_tempdir()
  .good_db(dir)
  r <- .run_validate(dir, start_fresh = "true")
  expect_equal(r$status, 0L)
  expect_true(file.exists(file.path(dir, "metadata.db")))
  expect_equal(r$env, character(0))
})
