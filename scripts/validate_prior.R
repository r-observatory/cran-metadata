#!/usr/bin/env Rscript
# Checks the downloaded metadata.db before update.R runs and exits 1 when the
# prior manifest cannot be parsed or the db cannot be read or has lost state.
# A manifest that cannot be parsed fails the check without opening the db.
# With START_FRESH=true a failed check removes the db and its manifest instead,
# and COLD_START=true goes to $GITHUB_ENV.
.file_arg <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
.script_dir <- if (length(.file_arg)) dirname(normalizePath(.file_arg[1])) else "scripts"
source(file.path(.script_dir, "helpers.R"))

args <- commandArgs(trailingOnly = TRUE)
db_path <- if (length(args) >= 1) args[1] else "metadata.db"
manifest_path <- if (length(args) >= 2) args[2] else "prior-manifest.json"

manifest <- tryCatch(read_prior_manifest(manifest_path), error = function(e) e)
res <- if (inherits(manifest, "error")) {
  list(ok = FALSE, problems = paste("prior manifest unreadable:", conditionMessage(manifest)),
       counts = integer(0))
} else {
  validate_prior_db(db_path, manifest)
}
for (t in names(res$counts)) cat(sprintf("  %s: %d rows\n", t, res$counts[[t]]))

action <- prior_db_action(res$ok, identical(Sys.getenv("START_FRESH"), "true"))
if (action == "keep") {
  cat("Prior metadata.db OK\n")
} else if (action == "discard") {
  for (p in res$problems) cat("::warning::", p, "\n", sep = "")
  file.remove(db_path)
  if (file.exists(manifest_path)) file.remove(manifest_path)
  env_file <- Sys.getenv("GITHUB_ENV")
  if (nzchar(env_file)) cat("COLD_START=true\n", file = env_file, append = TRUE)
  cat("start_fresh: removed the prior metadata.db; this run starts over\n")
} else {
  for (p in res$problems) cat("::error::", p, "\n", sep = "")
  cat("The prior metadata.db failed validation. Repair the release, or dispatch",
      "the workflow with start_fresh to discard it.\n")
  quit(status = 1)
}
