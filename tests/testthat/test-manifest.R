# Verify the integrity/completeness manifest core and JSON serialization.

# Self-sufficient: source helpers if the harness did not already load them.
if (!exists("summary_integrity_core", mode = "function")) {
  source(normalizePath(file.path("..", "..", "scripts", "helpers.R")))
}

# Build a tiny metadata.db fixture through a full connect/close cycle so the
# on-disk bytes are finalized before we hash them. The AUTOINCREMENT table
# forces SQLite to create an internal `sqlite_sequence` table, which lets us
# assert the `sqlite_%` filter excludes it.
build_fixture <- function(path) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(con, "CREATE TABLE authors (id INTEGER PRIMARY KEY AUTOINCREMENT, package TEXT)")
  DBI::dbExecute(con, "CREATE TABLE packages_enrichment (name TEXT PRIMARY KEY, url TEXT)")
  DBI::dbWriteTable(con, "authors",
    data.frame(package = c("a", "b", "c"), stringsAsFactors = FALSE),
    append = TRUE)
  DBI::dbWriteTable(con, "packages_enrichment",
    data.frame(name = c("x", "y"), url = c("u", "v"), stringsAsFactors = FALSE),
    append = TRUE)
  DBI::dbDisconnect(con)
  invisible(path)
}

test_that("summary_integrity_core reports the expected fields", {
  db <- withr::local_tempfile(fileext = ".db")
  build_fixture(db)

  core <- summary_integrity_core(db, complete = FALSE)

  # db_filename is the basename of the file.
  expect_identical(core$db_filename, basename(db))

  # db_bytes is a numeric size (double, not integer) equal to the real file
  # size, so a >2 GiB file serializes as a JSON number rather than "NA".
  expect_type(core$db_bytes, "double")
  expect_identical(core$db_bytes, file.size(db))
  expect_gt(core$db_bytes, 0)

  # complete is the honest boolean passed through by the caller.
  expect_false(core$complete)

  # tables maps every user table (NOT sqlite_%) to its row count. The internal
  # sqlite_sequence table created by AUTOINCREMENT must be excluded.
  expect_named(core$tables, c("authors", "packages_enrichment"))
  expect_false("sqlite_sequence" %in% names(core$tables))
  expect_equal(core$tables$authors, 3L)
  expect_equal(core$tables$packages_enrichment, 2L)
})

test_that("db_sha256 matches an independent sha256sum of the same bytes", {
  db <- withr::local_tempfile(fileext = ".db")
  build_fixture(db)

  core <- summary_integrity_core(db, complete = FALSE)

  # Independent hash via the system CLI, never through file_sha256's backend.
  sha_tool <- Sys.which("sha256sum")
  shasum_tool <- Sys.which("shasum")
  if (nzchar(sha_tool)) {
    ref <- system2(sha_tool, shQuote(db), stdout = TRUE)
  } else if (nzchar(shasum_tool)) {
    ref <- system2(shasum_tool, c("-a", "256", shQuote(db)), stdout = TRUE)
  } else {
    skip("neither sha256sum nor shasum is on PATH")
  }
  ref_hex <- tolower(sub("\\s.*$", "", ref[1]))

  expect_match(core$db_sha256, "^[0-9a-f]{64}$")
  expect_identical(core$db_sha256, ref_hex)
})

test_that("write_manifest serializes generated_at plus a top-level core", {
  db <- withr::local_tempfile(fileext = ".db")
  build_fixture(db)
  core <- summary_integrity_core(db, complete = FALSE)

  manifest <- withr::local_tempfile(fileext = ".json")
  write_manifest(manifest, core, generated_at = "2026-07-15T00:00:00Z")

  parsed <- jsonlite::fromJSON(manifest, simplifyVector = TRUE)
  expect_identical(parsed$generated_at, "2026-07-15T00:00:00Z")
  expect_identical(parsed$db_filename, basename(db))
  expect_identical(parsed$db_sha256, core$db_sha256)
  expect_false(parsed$complete)

  # db_bytes round-trips as a JSON number, not the string "NA".
  expect_true(is.numeric(parsed$db_bytes))
  expect_identical(as.numeric(parsed$db_bytes), file.size(db))

  # tables survive as an object of counts.
  expect_equal(parsed$tables$authors, 3L)
  expect_equal(parsed$tables$packages_enrichment, 2L)
})
