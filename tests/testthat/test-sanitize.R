# sanitize_df strips control characters but keeps tab, LF and CR.
if (!exists("sanitize_df", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

test_that("sanitize_df removes control characters and keeps tab, LF and CR", {
  df <- data.frame(
    txt = c("a\x01b\x0bc\x1fd", "keep\ttab\nlf\rcr", NA),
    n   = c(1L, 2L, 3L),
    stringsAsFactors = FALSE
  )
  out <- sanitize_df(df)
  expect_identical(out$txt, c("abcd", "keep\ttab\nlf\rcr", NA))
  expect_identical(out$n, c(1L, 2L, 3L))
})
