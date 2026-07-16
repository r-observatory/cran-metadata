library(testthat)

# Resolve scripts/ relative to this harness so tests run from any directory.
.harness_dir <- tryCatch(dirname(normalizePath(sys.frame(1)$ofile)), error = function(e) NA_character_)
if (is.na(.harness_dir)) {
  .file_arg <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .harness_dir <- if (length(.file_arg)) dirname(normalizePath(.file_arg[1])) else getwd()
}
scripts_dir <- normalizePath(file.path(.harness_dir, "..", "scripts"))
source(file.path(scripts_dir, "helpers.R"))

test_dir(file.path(.harness_dir, "testthat"), stop_on_failure = TRUE)
