# build_authors_df on a committed extract of tools::CRAN_authors_db().
if (!exists("build_authors_df", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

# 200 rows of tools::CRAN_authors_db() from 2026-09-25, built by
# fixtures/make-cran-authors-extract.R.
.extract <- function() {
  as.data.frame(jsonlite::fromJSON(test_path("fixtures", "cran-authors-extract.json")),
                stringsAsFactors = FALSE)
}
.row <- function(out, pkg, given) which(out$package == pkg & out$given == given)

test_that("the CRAN extract recovers 57 ORCID iDs in 35 packages and 1 ROR id", {
  raw <- .extract()
  out <- build_authors_df(raw)
  expect_identical(nrow(out), 200L)
  moved <- is.na(raw$ORCID) & !is.na(out$orcid)
  expect_identical(sum(moved), 57L)
  expect_identical(length(unique(out$package[moved])), 35L)
  expect_true(all(orcid_checksum_ok(out$orcid[moved])))
  expect_identical(sum(moved & is.na(out$comment)), 55L)
  expect_identical(sum(is.na(raw$ROR) & !is.na(out$ror_id)), 1L)
  expect_identical(attr(out, "recovered"), c(orcid = 57L, ror = 1L))
  expect_identical(sum(!is.na(raw$comment) & is.na(out$comment)), 56L)
})

test_that("named rows of the CRAN extract read as expected", {
  out <- build_authors_df(.extract())
  i <- .row(out, "FIRM", "Jingsi")
  expect_identical(out$orcid[i], "0000-0001-7059-4156")
  expect_identical(out$comment[i],
    "R package development and method implementation, ORCID: 0000-0001-7059-4156")
  i <- .row(out, "rcssci", "Zhiqiang")
  expect_identical(out$orcid[i], "0000-0001-7642-3286")
  expect_identical(out$comment[i], "ORCID = 0000-0001-7642-3286, wechat = Biostatistics-SCI")
  i <- .row(out, "AssociationExplorer2", "C\u00e9dric")
  expect_identical(out$orcid[i], "0000-0002-3150-3044")
  expect_identical(out$comment[i], NA_character_)
  i <- .row(out, "RSTr", "Centers for Disease Control and Prevention")
  expect_identical(out$ror_id[i], "042twtr12")
  expect_identical(out$comment[i], NA_character_)
  for (pkg in c("gap", "pQTLdata")) {
    i <- .row(out, pkg, "Jing Hua")
    expect_identical(out$orcid[i], "0000-0002-1463-5870")
    expect_identical(out$comment[i], "0000-0003-4930-3582")
  }
  i <- .row(out, "doclisting", "Posit Software, PBC")
  expect_identical(out$ror_id[i], "https://ror.org/03wc8by49")
  i <- .row(out, "AirportProblems", "Miguel \u00c1ngel")
  expect_identical(out$comment[i], "RGEAF. Departamento de Matem\u00e1ticas. Universidade de Vigo. Spain")
})

test_that("review links past character 120 survive whole", {
  out <- build_authors_df(.extract())
  link <- "github\\.com/(ropensci/(software-review|onboarding)|openjournals/joss-reviews)/issues/[0-9]+"
  m <- regexpr(link, out$comment, perl = TRUE)
  ends <- m + attr(m, "match.length") - 1L
  late <- which(m > 0 & ends > 120)
  expect_identical(length(late), 4L)
  expect_setequal(out$package[late], c("quadkeyr", "tradestatistics"))
  expect_setequal(substring(out$comment[late], m[late], ends[late]),
                  c("github.com/ropensci/software-review/issues/619",
                    "github.com/ropensci/onboarding/issues/217"))
})

test_that("email addresses inside comments are kept for the viewer to remove", {
  raw <- .extract()
  out <- build_authors_df(raw)
  has <- grepl("[[:alnum:]._%+-]+@[[:alnum:].-]+[.][a-z]{2,}", raw$comment)
  expect_identical(sum(has), 3L)
  expect_identical(out$comment[has], raw$comment[has])
})

test_that("the extract holds no real email address", {
  raw <- .extract()
  expect_true(all(is.na(raw$email)))
  cm <- raw$comment[!is.na(raw$comment)]
  found <- unlist(regmatches(cm, gregexpr("[[:alnum:]._%+-]+@[[:alnum:].-]+[.][[:alpha:]]{2,}", cm)))
  expect_identical(unique(found), "someone@example.org")
})

test_that("multi-line comments in the extract collapse to one line", {
  raw <- .extract()
  out <- build_authors_df(raw)
  expect_identical(sum(grepl("\n", raw$comment)), 17L)
  expect_false(any(grepl("[\n\t]|  ", out$comment)))
  i <- .row(out, "Boom", "Steven L.")
  expect_true(startsWith(out$comment[i],
    "Steven L. Scott is the sole author and creator of the BOOM project. Some code"))
})
