# Runs scripts/update.R offline in dir, with CRAN's readers returning the
# frames in fx, and returns the exit status and the output lines.
.update_script <- normalizePath(file.path("..", "..", "scripts", "update.R"))
.stub_profile <- normalizePath(file.path("fixtures", "run-update", "stubs.R"))

.run_update <- function(dir, fx, env = character(0)) {
  fx_path <- file.path(dir, "cran-fixture.rds")
  saveRDS(fx, fx_path)
  out <- withr::with_dir(dir, suppressWarnings(system2("Rscript",
    c(shQuote(.update_script), "metadata.db"),
    env = c(paste0("R_PROFILE_USER=", .stub_profile),
            paste0("CRAN_STUB_FIXTURE=", fx_path), env),
    stdout = TRUE, stderr = TRUE)))
  list(status = if (is.null(attr(out, "status"))) 0L else attr(out, "status"), out = out)
}

# A two-package CRAN as the stubs serve it. pkgB fails one flavor with flags.
.cran_fixture <- function(bounce = c(pkgA = "no", pkgB = "yes"),
                          status = c(pkgA = "OK", pkgB = "ERROR"),
                          check_version = c(pkgA = "1.0", pkgB = "2.0")) {
  pkgs <- names(bounce)
  list(
    check_results = data.frame(
      Flavor = "r-release-linux-x86_64", Package = pkgs, Version = unname(check_version),
      Priority = NA_character_, Maintainer = "M <m@example.org>",
      Status = factor(unname(status), levels = c("OK", "NOTE", "WARNING", "ERROR", "FAILURE"),
                      ordered = TRUE),
      Flags = ifelse(pkgs == "pkgB", "--no-vignettes", ""),
      T_install = 1, T_check = 2, T_total = 3, stringsAsFactors = FALSE),
    check_details = data.frame(
      Package = "pkgB", Version = "2.0", Flavor = "r-release-linux-x86_64", Check = "tests",
      Status = "ERROR", Output = "Error in f(): boom", stringsAsFactors = FALSE),
    check_issues = data.frame(Package = "pkgB", Version = "2.0", kind = "noSuggests",
                              href = "https://example.org/issue", stringsAsFactors = FALSE),
    authors = data.frame(Package = pkgs, given = "Ada", family = "Lovelace", email = NA,
                         role = "cre", comment = NA, stringsAsFactors = FALSE),
    package_db = data.frame(Package = pkgs, Version = c("1.0", "2.0")[seq_along(pkgs)],
                            Deadline = NA_character_, Bounce = unname(bounce), URL = NA,
                            BugReports = NA, stringsAsFactors = FALSE))
}

.db_query <- function(dir, sql) {
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "metadata.db"))
  on.exit(DBI::dbDisconnect(con))
  DBI::dbGetQuery(con, sql)
}
