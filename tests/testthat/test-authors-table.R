# create_authors_table owns the published authors schema.
if (!exists("create_authors_table", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

.authors_frame <- function(comment = c("Maintainer since 2020", NA)) {
  data.frame(package = c("pkgA", "pkgB"), given = c("Ada", "Ben"),
             family = c("Lovelace", "Ng"), email = c(NA, NA),
             role = c("aut, cre", "ctb"), orcid = c("0000-0002-1825-0097", NA),
             ror_id = c(NA, NA), comment = comment, stringsAsFactors = FALSE)
}

test_that("create_authors_table gives the published columns with a TEXT comment", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  create_authors_table(con)
  info <- DBI::dbGetQuery(con, "PRAGMA table_info(authors)")
  expect_identical(info$name, c("id", "package", "given", "family", "email",
                                "role", "orcid", "ror_id", "comment"))
  expect_identical(info$type[info$name == "comment"], "TEXT")
  idx <- DBI::dbGetQuery(con,
    "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'authors'")$name
  expect_setequal(idx, c("idx_authors_package", "idx_authors_name"))
})

test_that("build_authors_df output appends to the table as written", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  raw <- data.frame(given = c("Ada", "Ben"), family = c("Lovelace", "Ng"),
                    email = c(NA, NA), role = c("aut, cre", "ctb"),
                    comment = c("Maintainer since\n  2020", "ORCID: 0000-0002-1825-0097"),
                    ORCID = c(NA, NA), ROR = c(NA, NA),
                    package = c("pkgA", "pkgB"), stringsAsFactors = FALSE)
  create_authors_table(con)
  DBI::dbWriteTable(con, "authors", build_authors_df(raw), append = TRUE)
  got <- DBI::dbGetQuery(con, "SELECT package, orcid, comment FROM authors ORDER BY id")
  expect_identical(got$package, c("pkgA", "pkgB"))
  expect_identical(got$comment, c("Maintainer since 2020", NA))
  expect_identical(got$orcid, c(NA, "0000-0002-1825-0097"))
})

test_that("an all-NA comment column is stored as NULL, not as a typed value", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  create_authors_table(con)
  DBI::dbWriteTable(con, "authors", .authors_frame(comment = c(NA, NA)), append = TRUE)
  expect_identical(
    DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM authors WHERE comment IS NULL")$n, 2L)
  info <- DBI::dbGetQuery(con, "PRAGMA table_info(authors)")
  expect_identical(info$type[info$name == "comment"], "TEXT")
})

test_that("a prior authors table without comment is replaced, not altered", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  DBI::dbExecute(con, "CREATE TABLE authors (id INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL, given TEXT, family TEXT, email TEXT, role TEXT,
    orcid TEXT, ror_id TEXT)")
  DBI::dbExecute(con, "INSERT INTO authors (package, given) VALUES ('old', 'Old')")
  create_authors_table(con)
  expect_identical(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM authors")$n, 0L)
  expect_identical(DBI::dbListFields(con, "authors"),
                   c("id", "package", "given", "family", "email",
                     "role", "orcid", "ror_id", "comment"))
})

test_that("after a normalization error every other column is still written", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  raw <- data.frame(given = c("Ada", "Ben"), family = c("Lovelace", "Ng"),
                    email = c("ada@example.org", NA), role = c("aut, cre", "ctb"),
                    comment = c("ORCID: 0000-0002-1825-0097", "Wrote the C code"),
                    ORCID = c(NA, "0000-0001-5109-3700"), ROR = c("042twtr12", NA),
                    package = c("pkgA", "pkgA"), stringsAsFactors = FALSE)
  expect_output(
    out <- build_authors_df(raw, normalize = function(...) stop("forced failure")),
    "normalization error"
  )
  create_authors_table(con)
  DBI::dbWriteTable(con, "authors", out, append = TRUE)
  got <- DBI::dbGetQuery(con,
    "SELECT package, given, email, role, orcid, ror_id, comment FROM authors ORDER BY id")
  expect_identical(got$given, c("Ada", "Ben"))
  expect_identical(got$email, c("ada@example.org", NA))
  expect_identical(got$orcid, c(NA, "0000-0001-5109-3700"))
  expect_identical(got$ror_id, c("042twtr12", NA))
  expect_identical(got$comment, c(NA_character_, NA_character_))
})
