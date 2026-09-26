# Author comment normalization and ORCID / ROR recovery from the free comment.
# Kept byte-identical in cran-metadata and bioconductor-metadata.
if (!exists("normalize_author_comments", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

test_that("orcid_checksum_ok follows ISO 7064 MOD 11-2", {
  expect_identical(
    orcid_checksum_ok(c("0000-0002-1825-0097", "0000-0001-5109-3700",
                        "0000-0002-1694-233X", "0000-0002-1825-0098",
                        "0000-0002-1694-2339", NA, "not an id")),
    c(TRUE, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE)
  )
})

test_that("comment whitespace collapses to single spaces and blanks become NA", {
  expect_identical(
    collapse_comment_whitespace(c(" a\n  b\t c ", "one", "   ", "", "\n\t", NA,
                                  "line one\r\nline two\r\n", "old\rmac")),
    c("a b c", "one", NA, NA, NA, NA, "line one line two", "old mac")
  )
})

.norm <- function(comment, orcid = NA_character_, ror_id = NA_character_) {
  n <- length(comment)
  normalize_author_comments(comment, rep_len(orcid, n), rep_len(ror_id, n))
}

test_that("a lone valid ORCID iD moves to orcid and the comment empties", {
  inputs <- c("0000-0002-1825-0097",
              "ORCID: 0000-0002-1825-0097",
              "orcid=0000-0002-1825-0097.",
              "<https://orcid.org/0000-0002-1825-0097>",
              "ORCID = \"0000-0002-1825-0097\"",
              "http://orcid.org/0000-0002-1825-0097")
  r <- .norm(inputs)
  expect_identical(r$orcid, rep("0000-0002-1825-0097", length(inputs)))
  expect_identical(r$comment, rep(NA_character_, length(inputs)))
  expect_identical(r$n_orcid, length(inputs))
})

test_that("an iD beside other text moves and the comment stays verbatim", {
  r <- .norm("R package development, ORCID: 0000-0002-1694-233X")
  expect_identical(r$orcid, "0000-0002-1694-233X")
  expect_identical(r$comment, "R package development, ORCID: 0000-0002-1694-233X")
})

test_that("an iD is left in place when it fails the check, sits beside another iD, or orcid is set", {
  r <- .norm(c("0000-0002-1825-0098",
               "0000-0002-1825-0097 and 0000-0001-5109-3700",
               "10000-0002-1825-00977"))
  expect_identical(r$orcid, rep(NA_character_, 3))
  expect_identical(r$comment, c("0000-0002-1825-0098",
                                "0000-0002-1825-0097 and 0000-0001-5109-3700",
                                "10000-0002-1825-00977"))
  expect_identical(r$n_orcid, 0L)

  kept <- .norm("0000-0003-4930-3582", orcid = "0000-0002-1463-5870")
  expect_identical(kept$orcid, "0000-0002-1463-5870")
  expect_identical(kept$comment, "0000-0003-4930-3582")
})

test_that("the same iD written twice is one distinct iD and moves", {
  r <- .norm(c("ORCID: 0000-0002-1825-0097 <https://orcid.org/0000-0002-1825-0097>",
               "0000-0002-1825-0097 (0000-0002-1825-0097), University X"))
  expect_identical(r$orcid, rep("0000-0002-1825-0097", 2))
  expect_identical(r$comment,
                   c(NA, "0000-0002-1825-0097 (0000-0002-1825-0097), University X"))
  expect_identical(r$n_orcid, 2L)
})

test_that("an empty-string orcid counts as missing", {
  r <- .norm("ORCID: 0000-0002-1825-0097", orcid = "")
  expect_identical(r$orcid, "0000-0002-1825-0097")
  expect_identical(r$comment, NA_character_)
})

test_that("a ror.org id moves to ror_id when the column is empty", {
  r <- .norm(c("https://ror.org/042twtr12",
               "ROR: ror.org/042twtr12",
               "Funded through https://ror.org/042twtr12 since 2021"))
  expect_identical(r$ror_id, rep("042twtr12", 3))
  expect_identical(r$comment, c(NA, NA, "Funded through https://ror.org/042twtr12 since 2021"))
  expect_identical(r$n_ror, 3L)
})

test_that("a bare or www identifier URL, or an ORCID iD label, still empties the comment", {
  r <- .norm(c("orcid.org/0000-0002-1825-0097",
               "https://www.orcid.org/0000-0002-1825-0097/",
               "ORCID iD: 0000-0002-1825-0097",
               "ROR ID: ror.org/042twtr12"))
  expect_identical(r$orcid, c(rep("0000-0002-1825-0097", 3), NA))
  expect_identical(r$ror_id, c(NA, NA, NA, "042twtr12"))
  expect_identical(r$comment, rep(NA_character_, 4))
})

test_that("ror.org inside a longer host name is not a ROR id", {
  r <- .norm("Mirror at https://mirror.org/042twtr12")
  expect_identical(r$ror_id, NA_character_)
  expect_identical(r$comment, "Mirror at https://mirror.org/042twtr12")
  expect_identical(r$n_ror, 0L)
})

test_that("a ROR id is left in place when malformed, beside another id, or already set", {
  r <- .norm(c("https://ror.org/0liu12345",
               "https://ror.org/042twtr12 and https://ror.org/03wc8by49"))
  expect_identical(r$ror_id, rep(NA_character_, 2))
  expect_identical(r$n_ror, 0L)
  set <- .norm("https://ror.org/042twtr12", ror_id = "03wc8by49")
  expect_identical(set$ror_id, "03wc8by49")
  expect_identical(set$comment, "https://ror.org/042twtr12")
})

test_that("the same ROR id written twice is one distinct id and moves", {
  r <- .norm("https://ror.org/042twtr12 (ror.org/042twtr12)")
  expect_identical(r$ror_id, "042twtr12")
  expect_identical(r$comment, NA_character_)
  expect_identical(r$n_ror, 1L)
})

test_that("an ORCID and a ROR id in one comment both move", {
  r <- .norm("ORCID: 0000-0002-1825-0097, ROR: https://ror.org/042twtr12")
  expect_identical(r$orcid, "0000-0002-1825-0097")
  expect_identical(r$ror_id, "042twtr12")
  expect_identical(r$comment, NA_character_)
})

test_that("a long comment or one holding an email address is kept whole", {
  long <- paste(rep("word", 200), collapse = " ")
  mail <- "Contact ada@example.org about the C code"
  r <- .norm(c(long, mail))
  expect_identical(r$comment, c(long, mail))
})

# The same cases run in both repositories, so a helper edited in one fails here.
test_that("the shared comment cases give the pinned output", {
  cases <- jsonlite::fromJSON(test_path("fixtures", "author-comment-cases.json"))
  expect_identical(ORCID_ID_PATTERN, cases$orcid_id_pattern)
  expect_identical(ROR_ID_PATTERN, cases$ror_id_pattern)
  expect_identical(orcid_checksum_ok(cases$checksum$id), cases$checksum$ok)
  expect_identical(collapse_comment_whitespace(cases$whitespace$input),
                   as.character(cases$whitespace$want))
  n <- cases$normalize
  r <- normalize_author_comments(n$comment, as.character(n$orcid), as.character(n$ror_id))
  expect_identical(r$comment, as.character(n$want_comment))
  expect_identical(r$orcid, as.character(n$want_orcid))
  expect_identical(r$ror_id, as.character(n$want_ror_id))
  expect_identical(c(r$n_orcid, r$n_ror), c(cases$n_orcid, cases$n_ror))
})
