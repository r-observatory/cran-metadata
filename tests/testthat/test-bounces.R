# cran_maintainer_bounces: episodes of CRAN's Bounce flag.
if (!exists("write_bounces", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

.bpdb <- function(bounce, version = NULL) {
  pkgs <- names(bounce)
  data.frame(Package = pkgs, Version = if (is.null(version)) "1.0" else unname(version),
             Bounce = unname(bounce), stringsAsFactors = FALSE)
}
.brows <- function(con) DBI::dbGetQuery(con,
  "SELECT * FROM cran_maintainer_bounces ORDER BY package, episode_seq")
.bcon <- function() {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  withr::defer(DBI::dbDisconnect(con), envir = parent.frame())
  con
}

test_that("the first run opens every flagged package with an unknown onset", {
  con <- .bcon()
  r <- write_bounces(con, .bpdb(c(a = "yes", b = "no", c = "yes"), c("1.0", "2.0", "3.1")),
                     today = "2026-10-01")
  expect_false(r$skipped)
  expect_equal(r$new, 2L)
  got <- .brows(con)
  expect_equal(got$package, c("a", "c"))
  expect_equal(got$episode_seq, c(1L, 1L))
  expect_equal(got$version, c("1.0", "3.1"))
  expect_equal(got$onset_known, c(0L, 0L))
  expect_equal(got$first_seen, c("2026-10-01", "2026-10-01"))
  expect_true(all(is.na(got$resolved_on)))
})

test_that("a later run extends, opens with a known onset, clears and vanishes", {
  con <- .bcon()
  write_bounces(con, .bpdb(c(a = "yes", b = "yes", c = "yes", d = "no")), today = "2026-10-01")
  r <- write_bounces(con, .bpdb(c(a = "yes", b = "no", d = "yes")), today = "2026-10-02")
  expect_equal(c(r$new, r$extended, r$closed), c(1L, 1L, 2L))
  got <- .brows(con)
  a <- got[got$package == "a", ]
  expect_equal(a$last_seen, "2026-10-02")
  expect_true(is.na(a$outcome))
  b <- got[got$package == "b", ]
  expect_equal(c(b$outcome, b$resolved_on, b$last_seen), c("cleared", "2026-10-02", "2026-10-01"))
  cc <- got[got$package == "c", ]
  expect_equal(c(cc$outcome, cc$last_seen), c("vanished", "2026-10-01"))
  d <- got[got$package == "d", ]
  expect_equal(c(d$onset_known, d$episode_seq), c(1L, 1L))
  expect_equal(d$first_seen, "2026-10-02")
})

test_that("a package flagged again after clearing opens episode 2", {
  con <- .bcon()
  write_bounces(con, .bpdb(c(a = "yes", b = "no")), today = "2026-10-01")
  write_bounces(con, .bpdb(c(a = "no", b = "no")), today = "2026-10-02")
  write_bounces(con, .bpdb(c(a = "yes", b = "no"), c("1.1", "1.0")), today = "2026-10-05")
  got <- .brows(con)
  expect_equal(got$episode_seq, c(1L, 2L))
  expect_equal(got$outcome, c("cleared", NA))
  expect_equal(got$version, c("1.0", "1.1"))
  expect_equal(got$onset_known, c(0L, 1L))
})

test_that("a missing Bounce column skips and, on a first run, creates nothing", {
  con <- .bcon()
  r <- write_bounces(con, data.frame(Package = "a", Version = "1.0"), today = "2026-10-01")
  expect_true(r$skipped)
  expect_false(DBI::dbExistsTable(con, "cran_maintainer_bounces"))
  write_bounces(con, .bpdb(c(a = "yes")), today = "2026-10-02")
  expect_equal(.brows(con)$onset_known, 0L)
})

test_that("fewer than 99% yes or no values skips the diff", {
  con <- .bcon()
  vals <- c(rep("no", 97), "yes", "maybe", "")
  names(vals) <- paste0("p", 1:100)
  expect_true(write_bounces(con, .bpdb(vals), today = "2026-10-01")$skipped)
  vals[100] <- "no"
  expect_false(write_bounces(con, .bpdb(vals), today = "2026-10-01")$skipped)
})

test_that("with 20 or more open, an empty or more than halved flagged set skips", {
  con <- .bcon()
  flagged <- setNames(rep("yes", 30), paste0("p", 1:30))
  write_bounces(con, .bpdb(flagged), today = "2026-10-01")
  expect_true(write_bounces(con, .bpdb(setNames(rep("no", 30), paste0("p", 1:30))),
                            today = "2026-10-02")$skipped)
  sixty_drop <- flagged
  sixty_drop[1:18] <- "no"
  expect_true(write_bounces(con, .bpdb(sixty_drop), today = "2026-10-02")$skipped)
  expect_equal(sum(is.na(.brows(con)$resolved_on)), 30L)
  half <- flagged
  half[1:15] <- "no"
  expect_false(write_bounces(con, .bpdb(half), today = "2026-10-02")$skipped)
})

test_that("below 20 open episodes, clearing all of them proceeds", {
  con <- .bcon()
  write_bounces(con, .bpdb(setNames(rep("yes", 5), paste0("p", 1:5))), today = "2026-10-01")
  r <- write_bounces(con, .bpdb(setNames(rep("no", 5), paste0("p", 1:5))), today = "2026-10-02")
  expect_false(r$skipped)
  expect_equal(r$closed, 5L)
})

test_that("duplicate CRAN rows do not trip the open-episode index", {
  con <- .bcon()
  dup <- data.frame(Package = c("a", "a", "b"), Version = c("1.0", "1.0", "2.0"),
                    Bounce = c("yes", "yes", "no"), stringsAsFactors = FALSE)
  r <- write_bounces(con, dup, today = "2026-10-01")
  expect_equal(r$new, 1L)
  expect_error(DBI::dbExecute(con, "INSERT INTO cran_maintainer_bounces
    (package, episode_seq, onset_known, first_seen, last_seen)
    VALUES ('a', 9, 1, '2026-10-01', '2026-10-01')"), "UNIQUE")
})
