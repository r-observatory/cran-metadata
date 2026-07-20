# Episode diff + no-data floor for cran_check_deadlines.
if (!exists("compute_deadline_changes", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

# helpers to build the inputs compactly
.prior <- function(...) {
  rows <- list(...)
  if (length(rows) == 0) return(data.frame(package=character(0), episode_seq=integer(0),
    deadline=character(0), last_seen=character(0), stringsAsFactors=FALSE))
  do.call(rbind, lapply(rows, function(r) data.frame(package=r[[1]], episode_seq=as.integer(r[[2]]),
    deadline=r[[3]], last_seen=r[[4]], stringsAsFactors=FALSE)))
}
.snap <- function(...) {
  rows <- list(...)
  if (length(rows) == 0) return(data.frame(package=character(0), deadline=character(0),
    version=character(0), stringsAsFactors=FALSE))
  do.call(rbind, lapply(rows, function(r) data.frame(package=r[[1]], deadline=r[[2]],
    version=r[[3]], stringsAsFactors=FALSE)))
}

test_that("a brand-new deadline opens episode_seq 1 with today's dates", {
  ch <- compute_deadline_changes(
    prior_open = .prior(),
    snapshot   = .snap(c("pkgA", "2099-01-15", "1.0")),
    current_packages = c("pkgA"),
    worst_status_map = c(pkgA = "ERROR"),
    max_seq_map = setNames(integer(0), character(0)),
    today = "2098-12-01")
  expect_equal(nrow(ch$inserts), 1L)
  expect_equal(nrow(ch$updates), 0L)
  i <- ch$inserts
  expect_equal(i$package, "pkgA"); expect_equal(i$episode_seq, 1L)
  expect_equal(i$deadline, "2099-01-15"); expect_equal(i$worst_status, "ERROR")
  expect_equal(i$first_seen, "2098-12-01"); expect_equal(i$last_seen, "2098-12-01")
  expect_true(is.na(i$resolved_on)); expect_true(is.na(i$outcome)); expect_true(is.na(i$archived_on))
})

test_that("a re-observed deadline extends last_seen and the date, staying open", {
  ch <- compute_deadline_changes(
    prior_open = .prior(c("pkgA", 1, "2099-01-15", "2098-12-01")),
    snapshot   = .snap(c("pkgA", "2099-02-01", "1.0")),   # deadline extended
    current_packages = c("pkgA"),
    worst_status_map = c(pkgA = "ERROR"),
    max_seq_map = c(pkgA = 1L),
    today = "2098-12-10")
  expect_equal(nrow(ch$inserts), 0L); expect_equal(nrow(ch$updates), 1L)
  u <- ch$updates
  expect_equal(u$deadline, "2099-02-01"); expect_equal(u$last_seen, "2098-12-10")
  expect_true(is.na(u$resolved_on)); expect_true(is.na(u$outcome))
  expect_equal(u$package, "pkgA"); expect_equal(u$episode_seq, 1L)
})

test_that("a deadline that clears while the package stays on CRAN closes as met", {
  ch <- compute_deadline_changes(
    prior_open = .prior(c("pkgA", 1, "2099-01-15", "2098-12-10")),
    snapshot   = .snap(),                       # no deadline today
    current_packages = c("pkgA"),               # still on CRAN
    worst_status_map = setNames(character(0), character(0)),
    max_seq_map = c(pkgA = 1L),
    today = "2098-12-20")
  expect_equal(nrow(ch$updates), 1L)
  u <- ch$updates
  expect_equal(u$resolved_on, "2098-12-20"); expect_equal(u$outcome, "met")
  expect_equal(u$last_seen, "2098-12-10")      # NOT advanced (not seen today)
  expect_equal(u$deadline, "2099-01-15")       # unchanged
})

test_that("a deadline whose package left CRAN closes as vanished", {
  ch <- compute_deadline_changes(
    prior_open = .prior(c("pkgA", 1, "2099-01-15", "2098-12-10")),
    snapshot   = .snap(),
    current_packages = character(0),            # gone from CRAN
    worst_status_map = setNames(character(0), character(0)),
    max_seq_map = c(pkgA = 1L),
    today = "2098-12-20")
  u <- ch$updates
  expect_equal(u$outcome, "vanished"); expect_equal(u$resolved_on, "2098-12-20")
})

test_that("a re-appearing deadline opens a NEW episode (seq + 1)", {
  ch <- compute_deadline_changes(
    prior_open = .prior(),                       # prior episode already closed
    snapshot   = .snap(c("pkgA", "2099-05-01", "2.0")),
    current_packages = c("pkgA"),
    worst_status_map = c(pkgA = "WARNING"),
    max_seq_map = c(pkgA = 1L),                  # one prior (closed) episode
    today = "2099-04-15")
  expect_equal(nrow(ch$inserts), 1L)
  expect_equal(ch$inserts$episode_seq, 2L)
})

test_that("worst_status is NA when the package is not in the check map", {
  ch <- compute_deadline_changes(.prior(), .snap(c("pkgA","2099-01-15","1.0")),
    c("pkgA"), setNames(character(0), character(0)), setNames(integer(0), character(0)), "2098-12-01")
  expect_true(is.na(ch$inserts$worst_status))
})

test_that("deadline_snapshot_healthy skips on absent column, empty snapshot, or a big drop", {
  expect_false(deadline_snapshot_healthy(0L, 129L, has_col = FALSE))          # column gone
  expect_false(deadline_snapshot_healthy(0L, 129L, has_col = TRUE))           # empty vs prior
  expect_false(deadline_snapshot_healthy(50L, 129L, has_col = TRUE, 0.5))     # >50% drop
  expect_true(deadline_snapshot_healthy(120L, 129L, has_col = TRUE, 0.5))     # normal churn
  expect_true(deadline_snapshot_healthy(5L, 0L, has_col = TRUE))              # bootstrap (no prior)
})

test_that("small prior sets are not skipped: churn to zero is healthy below min_prior", {
  expect_true(deadline_snapshot_healthy(0L, 1L, has_col = TRUE))    # 1 open -> 0: normal resolution
  expect_true(deadline_snapshot_healthy(0L, 5L, has_col = TRUE))    # below min_prior
  expect_false(deadline_snapshot_healthy(0L, 20L, has_col = TRUE))  # at min_prior, empty snapshot -> skip
})

.pdb <- function(...) {   # minimal CRAN_package_db-shaped frame
  rows <- list(...)
  if (length(rows) == 0) return(data.frame(Package=character(0), Version=character(0),
    Deadline=character(0), stringsAsFactors=FALSE))
  do.call(rbind, lapply(rows, function(r) data.frame(Package=r[[1]], Version=r[[2]],
    Deadline=r[[3]], stringsAsFactors=FALSE)))
}
.open_rows <- function(con) DBI::dbGetQuery(con,
  "SELECT * FROM cran_check_deadlines WHERE resolved_on IS NULL ORDER BY package")
.all_rows <- function(con) DBI::dbGetQuery(con,
  "SELECT * FROM cran_check_deadlines ORDER BY package, episode_seq")

test_that("write_deadlines opens, extends, then closes across three runs", {
  db <- withr::local_tempfile(fileext = ".db")
  con <- DBI::dbConnect(RSQLite::SQLite(), db); on.exit(DBI::dbDisconnect(con))

  # Run 1: pkgA gets a deadline (NA = no deadline for pkgB, present as a current pkg)
  r1 <- write_deadlines(con, .pdb(c("pkgA","1.0","2099-01-15"), c("pkgB","2.0",NA)),
                        worst_status_map = c(pkgA="ERROR"), today = "2098-12-01")
  expect_false(r1$skipped); expect_equal(r1$new, 1L)
  o1 <- .open_rows(con); expect_equal(nrow(o1), 1L); expect_equal(o1$package, "pkgA")
  expect_equal(o1$episode_seq, 1L); expect_equal(o1$first_seen, "2098-12-01")

  # Run 2: pkgA deadline extended
  r2 <- write_deadlines(con, .pdb(c("pkgA","1.0","2099-02-01"), c("pkgB","2.0",NA)),
                        today = "2098-12-10")
  expect_equal(r2$extended, 1L)
  o2 <- .open_rows(con); expect_equal(o2$deadline, "2099-02-01"); expect_equal(o2$last_seen, "2098-12-10")

  # Run 3: pkgA fixed (deadline gone), still on CRAN -> met
  r3 <- write_deadlines(con, .pdb(c("pkgA","1.1","")), today = "2098-12-20")
  expect_equal(r3$closed, 1L)
  expect_equal(nrow(.open_rows(con)), 0L)
  closed <- .all_rows(con); expect_equal(closed$outcome, "met"); expect_equal(closed$resolved_on, "2098-12-20")
})

test_that("write_deadlines skips the diff on a no-data snapshot and preserves prior rows", {
  db <- withr::local_tempfile(fileext = ".db")
  con <- DBI::dbConnect(RSQLite::SQLite(), db); on.exit(DBI::dbDisconnect(con))
  write_deadlines(con, .pdb(c("pkgA","1.0","2099-01-15"), c("pkgB","2.0","2099-01-16")),
                  today = "2098-12-01")
  expect_equal(nrow(.open_rows(con)), 2L)
  # A pdb with the Deadline column entirely absent must NOT close the open episodes.
  pdb_nocol <- data.frame(Package=c("pkgA","pkgB"), Version=c("1.0","2.0"), stringsAsFactors=FALSE)
  r <- write_deadlines(con, pdb_nocol, today = "2098-12-02")
  expect_true(r$skipped)
  expect_equal(nrow(.open_rows(con)), 2L)   # both still open, unmutated
})

test_that("the open-episode unique index rejects a second open episode per package", {
  db <- withr::local_tempfile(fileext = ".db")
  con <- DBI::dbConnect(RSQLite::SQLite(), db); on.exit(DBI::dbDisconnect(con))
  write_deadlines(con, .pdb(c("pkgA","1.0","2099-01-15")), today = "2098-12-01")
  expect_error(
    DBI::dbExecute(con, "INSERT INTO cran_check_deadlines
      (package, episode_seq, deadline, first_seen, last_seen)
      VALUES ('pkgA', 99, '2099-03-01', '2098-12-01', '2098-12-01')"),
    "UNIQUE|constraint")
})

test_that("write_deadlines dedups duplicate package rows from CRAN_package_db", {
  db <- withr::local_tempfile(fileext = ".db")
  con <- DBI::dbConnect(RSQLite::SQLite(), db); on.exit(DBI::dbDisconnect(con))
  # pkgA appears twice with a deadline, as real CRAN_package_db() output can.
  pdb_dup <- data.frame(Package = c("pkgA", "pkgA", "pkgB"), Version = c("1.0", "1.0", "2.0"),
    Deadline = c("2099-01-15", "2099-01-15", "2099-02-01"), stringsAsFactors = FALSE)
  r <- write_deadlines(con, pdb_dup, today = "2098-12-01")   # must NOT throw on the unique index
  expect_false(r$skipped)
  expect_equal(r$new, 2L)                                     # pkgA once + pkgB
  expect_equal(nrow(.open_rows(con)), 2L)
  expect_equal(DBI::dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM cran_check_deadlines WHERE package = 'pkgA'")$n, 1L)
})
