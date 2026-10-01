# The update workflow fails closed on a bad prior metadata.db.
.workflow <- function() {
  yaml::read_yaml(normalizePath(file.path("..", "..", ".github", "workflows", "update.yml")))
}
.steps <- function(wf) wf$jobs$update$steps
.step <- function(wf, name) {
  s <- Filter(function(x) identical(x$name, name), .steps(wf))
  expect_length(s, 1L)
  s[[1]]
}

# Stands in for gh in the download step. STUB_FAIL=list or assets makes that
# listing fail as an API error does; any call the step should not make exits 3.
.gh_stub <- r"---[#!/bin/sh
case "$*" in
  "release list --limit 1 --exclude-drafts")
    [ "$STUB_FAIL" = list ] && { echo "HTTP 502" >&2; exit 1; }
    [ -z "$STUB_NO_RELEASE" ] && printf 'CRAN Metadata v1\tLatest\tv1\t2026-09-30\n'
    exit 0 ;;
  "release view --json tagName --jq .tagName") echo v1 ;;
  "release view v1 --json assets --jq .assets[].name")
    [ "$STUB_FAIL" = assets ] && { echo "HTTP 502" >&2; exit 1; }
    printf '%s\n' $STUB_ASSETS ;;
  "release download v1 --pattern metadata.db") echo db > metadata.db ;;
  "release download v1 --pattern manifest.json --output prior-manifest.json") echo "{}" > prior-manifest.json ;;
  *) echo "unexpected gh call: $*" >&2; exit 3 ;;
esac
]---"

# Runs the download step's own script under bash with the stub first on PATH.
.run_download <- function(dir, fail = "", assets = "manifest.json metadata.db",
                          no_release = FALSE) {
  bin <- file.path(dir, "bin")
  work <- file.path(dir, "work")
  dir.create(bin)
  dir.create(work)
  writeLines(.gh_stub, file.path(bin, "gh"))
  Sys.chmod(file.path(bin, "gh"), "755")
  script <- file.path(dir, "download.sh")
  writeLines(.step(.workflow(), "Download previous database")$run, script)
  withr::local_dir(work)
  withr::local_envvar(PATH = paste(bin, Sys.getenv("PATH"), sep = .Platform$path.sep),
                      STUB_FAIL = fail, STUB_ASSETS = assets,
                      STUB_NO_RELEASE = if (no_release) "1" else "")
  out <- suppressWarnings(system2("bash", shQuote(script), stdout = TRUE, stderr = TRUE))
  list(status = if (is.null(attr(out, "status"))) 0L else attr(out, "status"),
       out = out, work = work, files = sort(list.files(work)))
}

test_that("start_fresh is a boolean dispatch input that defaults to false", {
  wf <- .workflow()
  on <- if (!is.null(wf[["on"]])) wf[["on"]] else wf[["TRUE"]]
  input <- on$workflow_dispatch$inputs$start_fresh
  expect_equal(input$type, "boolean")
  expect_false(input$default)
})

test_that("the db and its manifest come from one resolved tag", {
  run <- .step(.workflow(), "Download previous database")$run
  expect_match(run, 'tag="$(gh release view --json tagName --jq .tagName)"', fixed = TRUE)
  expect_match(run, 'gh release download "$tag" --pattern "metadata.db"', fixed = TRUE)
  expect_match(run, 'assets="$(gh release view "$tag" --json assets --jq \'.assets[].name\')"', fixed = TRUE)
  expect_match(run, 'if grep -qx "manifest.json" <<<"$assets"; then', fixed = TRUE)
  expect_match(run, '--pattern "manifest.json" --output prior-manifest.json', fixed = TRUE)
  expect_match(run, "> prior-tag.txt", fixed = TRUE)
  expect_match(run, "set -euo pipefail", fixed = TRUE)
})

test_that("an API error on either release listing fails the download step", {
  for (fail in c("list", "assets")) {
    r <- .run_download(withr::local_tempdir(), fail = fail)
    expect_equal(r$status, 1L)
    expect_false("prior-manifest.json" %in% r$files)
    expect_false("prior-tag.txt" %in% r$files)
  }
})

test_that("the download step takes the manifest only when the release lists one", {
  r <- .run_download(withr::local_tempdir())
  expect_equal(r$status, 0L)
  expect_equal(r$files, c("metadata.db", "prior-manifest.json", "prior-tag.txt"))
  expect_equal(readLines(file.path(r$work, "prior-tag.txt")), "v1")
  r <- .run_download(withr::local_tempdir(), assets = "metadata.db")
  expect_equal(r$status, 0L)
  expect_equal(r$files, c("metadata.db", "prior-tag.txt"))
})

test_that("no release at all is a first run", {
  r <- .run_download(withr::local_tempdir(), no_release = TRUE)
  expect_equal(r$status, 0L)
  expect_equal(r$files, character(0))
})

test_that("validation runs the fail-closed script and never deletes the db itself", {
  s <- .step(.workflow(), "Validate downloaded database")
  expect_equal(s[["if"]], "hashFiles('metadata.db') != ''")
  expect_equal(s$env$START_FRESH, "${{ inputs.start_fresh }}")
  expect_equal(s$run, "Rscript scripts/validate_prior.R metadata.db prior-manifest.json")
  expect_false(grepl("file.remove", s$run, fixed = TRUE))
})

test_that("download, validation, update and release run in that order", {
  names <- vapply(.steps(.workflow()), function(s) s$name, character(1))
  pos <- match(c("Download previous database", "Validate downloaded database",
                 "Run update script", "Create release"), names)
  expect_false(anyNA(pos))
  expect_equal(pos, sort(pos))
})
