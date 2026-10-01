# cran_check_results keeps the checked Version and the Flags, and the details
# JSON in check_status_history names both.
if (!exists("build_check_results_df", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

.raw_results <- function() {
  data.frame(
    Flavor = c("r-devel-windows-x86_64", "r-release-linux-x86_64", NA),
    Package = c("pkgA", "pkgA", "pkgB"), Version = c("1.4.3", "1.4.2", "0.1"),
    Priority = NA_character_, Maintainer = "M <m@example.org>",
    Status = factor(c("OK", "ERROR", "OK"), levels = c("OK", "NOTE", "WARNING", "ERROR", "FAILURE"),
                    ordered = TRUE),
    Flags = c("--no-vignettes", "", ""), T_install = c(1.5, 2, 3), T_check = c(10, 20, 30),
    T_total = c(11.5, 22, 33), stringsAsFactors = FALSE)
}

test_that("build_check_results_df maps Version and Flags and stores blanks as NULL", {
  got <- build_check_results_df(.raw_results())
  expect_equal(names(got), c("package", "flavor", "status", "tinstall", "tcheck", "ttotal",
                             "version", "flags"))
  expect_equal(nrow(got), 2L)
  expect_equal(got$status, c("OK", "ERROR"))
  expect_equal(got$version, c("1.4.3", "1.4.2"))
  expect_equal(got$flags, c("--no-vignettes", NA))
  expect_equal(got$ttotal, c(11.5, 22))
})

test_that("frames without Version or Flags still build, with those columns NULL", {
  raw <- .raw_results()[, c("Flavor", "Package", "Status", "T_install", "T_check", "T_total")]
  got <- build_check_results_df(raw)
  expect_true(all(is.na(got$version)))
  expect_true(all(is.na(got$flags)))
})

test_that("the rebuilt table takes the frame with both new columns last", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  create_check_results_table(con)
  DBI::dbWriteTable(con, "cran_check_results", build_check_results_df(.raw_results()), append = TRUE)
  expect_equal(DBI::dbListFields(con, "cran_check_results"),
               c("package", "flavor", "status", "tinstall", "tcheck", "ttotal", "version", "flags"))
  got <- DBI::dbGetQuery(con, "SELECT version, flags FROM cran_check_results ORDER BY flavor")
  expect_equal(got$version, c("1.4.3", "1.4.2"))
  expect_equal(got$flags, c("--no-vignettes", NA))
})

test_that("a details entry carries version and flags and stays valid JSON", {
  e <- check_detail_entry("r-devel-windows-x86_64", "ERROR", "tests", "Error in \"f\"()\n",
                          "1.4.3", "--no-tests")
  parsed <- jsonlite::fromJSON(e)
  expect_equal(names(parsed), c("flavor", "status", "check_name", "output", "version", "flags"))
  expect_equal(parsed$version, "1.4.3")
  expect_equal(parsed$flags, "--no-tests")
  expect_equal(parsed$output, "Error in \"f\"()\n")
  blank <- jsonlite::fromJSON(check_detail_entry("f", "NOTE", "", "", NA, NULL))
  expect_equal(blank$version, "")
  expect_equal(blank$flags, "")
})
