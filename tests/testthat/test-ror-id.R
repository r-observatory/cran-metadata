# authors.ror_id holds the bare ROR id, whatever form CRAN gives it in.
if (!exists("build_authors_df", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

test_that("the URL-form ROR ids CRAN gives reduce to the bare id", {
  # Every distinct URL-form ROR in tools::CRAN_authors_db() on 2026-09-27.
  expect_identical(
    normalize_ror_id(c("https://ror.org/03wc8by49", "https://ror.org/00scbd467",
                       "https://ror.org/019jywm96")),
    c("03wc8by49", "00scbd467", "019jywm96"))
})

test_that("http, www, a bare host, a trailing slash and host case all reduce", {
  expect_identical(
    normalize_ror_id(c("http://ror.org/03wc8by49", "https://www.ror.org/03wc8by49",
                       "ror.org/03wc8by49", "https://ror.org/03wc8by49/",
                       "HTTPS://ROR.org/03wc8by49", " https://ror.org/03wc8by49 ",
                       "03wc8by49/")),
    rep("03wc8by49", 7))
})

test_that("a bare id and NA pass through", {
  expect_identical(normalize_ror_id(c("042twtr12", NA)), c("042twtr12", NA))
  expect_identical(normalize_ror_id(character(0)), character(0))
})

test_that("anything that does not reduce to a ROR id is left as given", {
  odd <- c("https://ror.org/0liu12345", "https://ror.org/03WC8BY49",
           "https://mirror.org/03wc8by49", "https://ror.org/03wc8by49/extra",
           "https://ror.org/03wc8by49//", "not a ror id", "")
  expect_identical(normalize_ror_id(odd), odd)
})

.ror_raw <- function(ror, package = paste0("pkg", seq_along(ror))) {
  n <- length(ror)
  data.frame(given = rep("Org", n), family = NA_character_, email = NA_character_,
             role = rep("fnd", n), comment = NA_character_, ORCID = NA_character_,
             ROR = ror, package = package, stringsAsFactors = FALSE)
}

test_that("build_authors_df stores the bare id and counts the values it reduced", {
  raw <- .ror_raw(c("https://ror.org/03wc8by49", "https://ror.org/00scbd467",
                    "042twtr12", NA),
                  package = c("purrr", "PGRdup", "RSTr", "pkgD"))
  expect_silent(out <- build_authors_df(raw))
  expect_identical(out$ror_id, c("03wc8by49", "00scbd467", "042twtr12", NA))
  expect_identical(attr(out, "ror_ids_reduced"), 2L)
})

test_that("build_authors_df keeps and logs a ror_id it cannot reduce", {
  raw <- .ror_raw(c("https://ror.org/03wc8by49", "https://ror.org/0liu12345"),
                  package = c("purrr", "pkgX"))
  expect_output(out <- build_authors_df(raw),
                "1 ror_id value kept as given.*pkgX: https://ror.org/0liu12345")
  expect_identical(out$ror_id, c("03wc8by49", "https://ror.org/0liu12345"))
  expect_identical(attr(out, "ror_ids_reduced"), 1L)
})

test_that("a bare ror_id is what lands in the authors table", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  create_authors_table(con)
  raw <- .ror_raw(c("https://ror.org/019jywm96", NA), package = c("spocc", "pkgB"))
  raw$comment[2] <- "ROR: https://ror.org/042twtr12"
  DBI::dbWriteTable(con, "authors", build_authors_df(raw), append = TRUE)
  got <- DBI::dbGetQuery(con, "SELECT package, ror_id, comment FROM authors ORDER BY id")
  expect_identical(got$ror_id, c("019jywm96", "042twtr12"))
  expect_identical(got$comment, c(NA_character_, NA_character_))
})

test_that("the ror_id is still bare when comment normalization fails", {
  raw <- .ror_raw("https://ror.org/03wc8by49", package = "doclisting")
  expect_output(
    out <- build_authors_df(raw, normalize = function(...) stop("forced failure")),
    "normalization error")
  expect_identical(out$ror_id, "03wc8by49")
})
