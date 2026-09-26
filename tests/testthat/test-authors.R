# build_authors_df: the authors write frame built from tools::CRAN_authors_db().
if (!exists("build_authors_df", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

.cran_authors <- function() {
  data.frame(
    given   = c("Ada", "Ben", "Cy"),
    family  = c("Lovelace", "Ng", "Oh"),
    email   = c("ada@example.org", NA, NA),
    role    = c("aut, cre", "ctb", "fnd"),
    comment = c("Maintainer since\n  2020", "ORCID: 0000-0002-1825-0097", NA),
    ORCID   = c(NA, NA, NA),
    ROR     = c(NA, NA, "042twtr12"),
    package = c("pkgA", "pkgA", "pkgB"),
    stringsAsFactors = FALSE
  )
}

test_that("build_authors_df maps CRAN columns in the published order", {
  out <- build_authors_df(.cran_authors())
  expect_named(out, c("package", "given", "family", "email", "role",
                      "orcid", "ror_id", "comment"))
  expect_identical(out$package, c("pkgA", "pkgA", "pkgB"))
  expect_identical(out$comment, c("Maintainer since 2020", NA, NA))
  expect_identical(out$orcid, c(NA, "0000-0002-1825-0097", NA))
  expect_identical(out$ror_id, c(NA, NA, "042twtr12"))
  expect_identical(attr(out, "recovered"), c(orcid = 1L, ror = 0L))
})

test_that("rows without a package are dropped", {
  a <- .cran_authors()
  a$package[2] <- NA
  out <- build_authors_df(a)
  expect_identical(out$given, c("Ada", "Cy"))
  expect_identical(rownames(out), c("1", "2"))
})

test_that("a missing comment column gives NA comments and keeps the rest", {
  a <- .cran_authors()
  a$comment <- NULL
  out <- build_authors_df(a)
  expect_identical(out$comment, rep(NA_character_, 3))
  expect_identical(out$ror_id, c(NA, NA, "042twtr12"))
})

test_that("a list comment column is joined with a comma", {
  a <- .cran_authors()
  a$comment <- I(list(c("first", "second"), NULL, NA))
  out <- build_authors_df(a)
  expect_identical(out$comment, c("first, second", NA, NA))
})

test_that("Windows and old Mac line breaks in a comment become single spaces", {
  a <- .cran_authors()
  a$comment[1] <- "Maintainer since\r\n2020,\rretired 2025"
  out <- build_authors_df(a)
  expect_identical(out$comment[1], "Maintainer since 2020, retired 2025")
})

test_that("an empty CRAN author database gives an empty frame with every column", {
  out <- build_authors_df(.cran_authors()[0, ])
  expect_identical(nrow(out), 0L)
  expect_named(out, c("package", "given", "family", "email", "role",
                      "orcid", "ror_id", "comment"))
  expect_identical(attr(out, "recovered"), c(orcid = 0L, ror = 0L))
})

test_that("invalid UTF-8 in a comment empties comment and never stops the table", {
  a <- .cran_authors()
  bad <- "Universit\xe9 de Vigo"
  Encoding(bad) <- "UTF-8"
  a$comment[1] <- bad
  expect_output(out <- build_authors_df(a), "normalization error: .*invalid UTF-8")
  expect_identical(nrow(out), 3L)
  expect_identical(out$comment, rep(NA_character_, 3))
  expect_identical(out$given, c("Ada", "Ben", "Cy"))
  expect_identical(out$ror_id, c(NA, NA, "042twtr12"))
})

test_that("a normalization error empties comment and keeps CRAN's identifiers", {
  a <- .cran_authors()
  a$ORCID[1] <- "0000-0001-5109-3700"
  expect_output(
    out <- build_authors_df(a, normalize = function(...) stop("forced failure")),
    "author comments left empty after a normalization error: forced failure"
  )
  expect_identical(nrow(out), 3L)
  expect_identical(out$comment, rep(NA_character_, 3))
  expect_identical(out$orcid, c("0000-0001-5109-3700", NA, NA))
  expect_identical(out$ror_id, c(NA, NA, "042twtr12"))
  expect_identical(out$email, c("ada@example.org", NA, NA))
  expect_identical(attr(out, "recovered"), c(orcid = 0L, ror = 0L))
})

test_that("a normalizer returning the wrong length counts as an error", {
  short <- function(comment, orcid, ror_id) {
    list(comment = comment[1], orcid = orcid, ror_id = ror_id, n_orcid = 0L, n_ror = 0L)
  }
  expect_output(out <- build_authors_df(.cran_authors(), normalize = short),
                "normalization error")
  expect_identical(out$comment, rep(NA_character_, 3))
})
