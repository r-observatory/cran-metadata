#!/usr/bin/env Rscript
# CRAN Metadata — fetch check results, authors, enrichment, check history,
# archival reasons, and NEWS daily. Writes to SQLite (metadata.db).

options(timeout = 120)

library(RSQLite)

# ---------------------------------------------------------------------------
# CLI argument: path to the SQLite database
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
db_path <- if (length(args) >= 1) args[1] else "metadata.db"

cat("Database path:", db_path, "\n")

# ---------------------------------------------------------------------------
# Connect and configure SQLite
# ---------------------------------------------------------------------------
con <- dbConnect(SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

dbExecute(con, "PRAGMA journal_mode=WAL")
dbExecute(con, "PRAGMA synchronous=NORMAL")

# ---------------------------------------------------------------------------
# JSON escape helper
# ---------------------------------------------------------------------------
json_escape <- function(s) {
  s <- gsub("\\\\", "\\\\\\\\", s)
  s <- gsub('"', '\\\\"', s)
  s <- gsub("\n", "\\\\n", s)
  s <- gsub("\t", "\\\\t", s)
  s <- gsub("\r", "\\\\r", s)
  s
}

# ---------------------------------------------------------------------------
# Create append-only table (never dropped)
# ---------------------------------------------------------------------------
dbExecute(con, "
CREATE TABLE IF NOT EXISTS check_status_history (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  package         TEXT NOT NULL,
  status          TEXT NOT NULL,
  flavor_summary  TEXT,
  details         TEXT,
  detected_at     TEXT NOT NULL
)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_csh_package  ON check_status_history (package)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_csh_detected ON check_status_history (detected_at)")

# ---------------------------------------------------------------------------
# Tracking variables for release notes
# ---------------------------------------------------------------------------
counts <- list(
  check_results   = 0L,
  check_details   = 0L,
  check_issues    = 0L,
  history_changes = 0L,
  authors         = 0L,
  enrichment      = 0L,
  removal_reasons = 0L,
  package_news    = 0L
)

# =========================================================================
# 1. Check Results
# =========================================================================
cat("\n=== 1. CRAN Check Results ===\n")
results_df <- NULL
tryCatch({
  results_df <- tools::CRAN_check_results()
  cat("  Fetched", nrow(results_df), "rows\n")

  dbExecute(con, "DROP TABLE IF EXISTS cran_check_results")
  dbExecute(con, "
  CREATE TABLE cran_check_results (
    package  TEXT NOT NULL,
    flavor   TEXT NOT NULL,
    status   TEXT NOT NULL,
    tinstall REAL,
    tcheck   REAL,
    ttotal   REAL,
    PRIMARY KEY (package, flavor)
  )")
  dbExecute(con, "CREATE INDEX idx_ccr_status ON cran_check_results (status)")

  # Rename columns to match schema
  write_df <- data.frame(
    package  = results_df$Package,
    flavor   = results_df$Flavor,
    status   = results_df$Status,
    tinstall = as.numeric(results_df$T_install),
    tcheck   = as.numeric(results_df$T_check),
    ttotal   = as.numeric(results_df$T_total),
    stringsAsFactors = FALSE
  )

  dbBegin(con)
  dbWriteTable(con, "cran_check_results", write_df, append = TRUE)
  dbCommit(con)
  counts$check_results <- nrow(write_df)
  cat("  Wrote", nrow(write_df), "check results\n")
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 2. Check Details
# =========================================================================
cat("\n=== 2. CRAN Check Details ===\n")
check_details_df <- NULL
tryCatch({
  check_details_df <- as.data.frame(tools::CRAN_check_details())
  cat("  Fetched", nrow(check_details_df), "rows\n")

  dbExecute(con, "DROP TABLE IF EXISTS cran_check_details")
  dbExecute(con, "
  CREATE TABLE cran_check_details (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    package    TEXT NOT NULL,
    flavor     TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status     TEXT NOT NULL,
    output     TEXT
  )")
  dbExecute(con, "CREATE INDEX idx_ccd_package ON cran_check_details (package)")

  write_df <- data.frame(
    package    = check_details_df$Package,
    flavor     = check_details_df$Flavor,
    check_name = if ("Check" %in% names(check_details_df)) check_details_df$Check else NA_character_,
    status     = check_details_df$Status,
    output     = if ("Output" %in% names(check_details_df)) check_details_df$Output else NA_character_,
    stringsAsFactors = FALSE
  )

  dbBegin(con)
  dbWriteTable(con, "cran_check_details", write_df, append = TRUE)
  dbCommit(con)
  counts$check_details <- nrow(write_df)
  cat("  Wrote", nrow(write_df), "check details\n")
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 3. Check Status History (append-only, change detection)
# =========================================================================
cat("\n=== 3. Check Status History ===\n")
tryCatch({
  if (!is.null(results_df) && is.data.frame(results_df) && nrow(results_df) > 0) {
    status_levels <- c("OK" = 0L, "NOTE" = 1L, "WARNING" = 2L, "ERROR" = 3L)

    pkg_list <- unique(results_df$Package)
    n_pkgs <- length(pkg_list)

    # Vectorised worst-status computation using split
    pkg_factor <- factor(results_df$Package, levels = pkg_list)
    severity_vec <- ifelse(results_df$Status %in% names(status_levels),
                           status_levels[results_df$Status], -1L)

    # Worst severity per package
    worst_sev <- tapply(severity_vec, pkg_factor, max)
    sev_to_status <- c("OK", "NOTE", "WARNING", "ERROR")
    worst_status <- sev_to_status[worst_sev + 1L]

    # Flavor summary: count of each status per package as JSON
    flavor_summary <- character(n_pkgs)
    status_split <- split(results_df$Status, pkg_factor)
    for (i in seq_along(pkg_list)) {
      tbl <- table(status_split[[i]])
      pairs <- paste0('"', names(tbl), '":', as.integer(tbl))
      flavor_summary[i] <- paste0("{", paste(pairs, collapse = ","), "}")
    }

    # Details: JSON array of non-OK entries enriched from check_details
    details_json <- character(n_pkgs)
    for (i in seq_along(pkg_list)) {
      pkg <- pkg_list[i]
      pkg_rows <- results_df[results_df$Package == pkg, , drop = FALSE]
      non_ok <- pkg_rows[pkg_rows$Status != "OK", , drop = FALSE]

      if (nrow(non_ok) == 0) {
        details_json[i] <- "[]"
        next
      }

      entries <- vapply(seq_len(nrow(non_ok)), function(j) {
        flav <- non_ok$Flavor[j]
        stat <- non_ok$Status[j]
        chk <- ""
        out <- ""
        if (!is.null(check_details_df) && nrow(check_details_df) > 0) {
          match_rows <- check_details_df[
            check_details_df$Package == pkg & check_details_df$Flavor == flav, ,
            drop = FALSE
          ]
          if (nrow(match_rows) > 0) {
            if ("Check" %in% names(match_rows)) chk <- as.character(match_rows$Check[1])
            if ("Output" %in% names(match_rows)) {
              out_raw <- match_rows$Output[1]
              if (!is.na(out_raw)) out <- substring(out_raw, 1, 500)
            }
          }
        }
        sprintf('{"flavor":"%s","status":"%s","check_name":"%s","output":"%s"}',
                json_escape(flav), json_escape(stat),
                json_escape(chk), json_escape(out))
      }, character(1))
      details_json[i] <- paste0("[", paste(entries, collapse = ","), "]")
    }

    # Load previous latest status per package
    prev_status <- dbGetQuery(con, "
      SELECT package, status
      FROM check_status_history
      WHERE id IN (
        SELECT MAX(id) FROM check_status_history GROUP BY package
      )
    ")
    prev_map <- setNames(prev_status$status, prev_status$package)

    # Detect changes: new packages or status changed
    changed_mask <- vapply(seq_along(pkg_list), function(i) {
      pkg <- pkg_list[i]
      if (is.na(prev_map[pkg])) return(TRUE)  # new package
      prev_map[pkg] != worst_status[i]         # status changed
    }, logical(1))

    changed_idx <- which(changed_mask)

    detected_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

    if (length(changed_idx) > 0) {
      changes_df <- data.frame(
        package        = pkg_list[changed_idx],
        status         = worst_status[changed_idx],
        flavor_summary = flavor_summary[changed_idx],
        details        = details_json[changed_idx],
        detected_at    = detected_at,
        stringsAsFactors = FALSE
      )

      # Insert with dedup guard
      dbBegin(con)
      stmt <- dbSendStatement(con, "
        INSERT INTO check_status_history (package, status, flavor_summary, details, detected_at)
        SELECT ?1, ?2, ?3, ?4, ?5
        WHERE NOT EXISTS (
          SELECT 1 FROM check_status_history
          WHERE package = ?1 AND detected_at = ?5 AND status = ?2
        )
      ")
      for (i in seq_len(nrow(changes_df))) {
        dbBind(stmt, list(
          changes_df$package[i],
          changes_df$status[i],
          changes_df$flavor_summary[i],
          changes_df$details[i],
          changes_df$detected_at[i]
        ))
      }
      dbClearResult(stmt)
      dbCommit(con)

      counts$history_changes <- length(changed_idx)
      cat("  Inserted", length(changed_idx), "status changes\n")
    } else {
      cat("  No status changes detected\n")
    }
  } else {
    cat("  No check results available, skipping history\n")
  }
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 4. Check Issues
# =========================================================================
cat("\n=== 4. CRAN Check Issues ===\n")
tryCatch({
  issues <- tools::CRAN_check_issues()
  cat("  Fetched", nrow(issues), "rows\n")

  dbExecute(con, "DROP TABLE IF EXISTS cran_check_issues")
  dbExecute(con, "
  CREATE TABLE cran_check_issues (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL,
    version TEXT,
    kind    TEXT NOT NULL,
    href    TEXT
  )")
  dbExecute(con, "CREATE INDEX idx_cci_package ON cran_check_issues (package)")

  write_df <- data.frame(
    package = issues$Package,
    version = if ("Version" %in% names(issues)) issues$Version else NA_character_,
    kind    = issues$Kind,
    href    = if ("href" %in% names(issues)) issues$href else NA_character_,
    stringsAsFactors = FALSE
  )

  dbBegin(con)
  dbWriteTable(con, "cran_check_issues", write_df, append = TRUE)
  dbCommit(con)
  counts$check_issues <- nrow(write_df)
  cat("  Wrote", nrow(write_df), "check issues\n")
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 5. Authors
# =========================================================================
cat("\n=== 5. Authors ===\n")
tryCatch({
  authors_raw <- tools::CRAN_authors_db()
  authors_df <- as.data.frame(authors_raw)
  cat("  Fetched", nrow(authors_df), "rows\n")

  dbExecute(con, "DROP TABLE IF EXISTS authors")
  dbExecute(con, "
  CREATE TABLE authors (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL,
    given   TEXT,
    family  TEXT,
    email   TEXT,
    role    TEXT,
    orcid   TEXT,
    ror_id  TEXT
  )")
  dbExecute(con, "CREATE INDEX idx_authors_package ON authors (package)")
  dbExecute(con, "CREATE INDEX idx_authors_name    ON authors (family, given)")

  # Build write data — handle columns that may not exist
  safe_col <- function(df, col) {
    if (col %in% names(df)) as.character(df[[col]]) else NA_character_
  }

  # The role column might be a list; collapse to comma-separated string
  role_col <- safe_col(authors_df, "role")
  if (is.list(authors_df$role)) {
    role_col <- vapply(authors_df$role, function(r) {
      if (is.null(r) || all(is.na(r))) NA_character_
      else paste(r, collapse = ", ")
    }, character(1))
  }

  # Email might also be a list
  email_col <- safe_col(authors_df, "email")
  if ("email" %in% names(authors_df) && is.list(authors_df$email)) {
    email_col <- vapply(authors_df$email, function(e) {
      if (is.null(e) || all(is.na(e))) NA_character_
      else paste(e, collapse = ", ")
    }, character(1))
  }

  write_df <- data.frame(
    package = safe_col(authors_df, "Package"),
    given   = safe_col(authors_df, "given"),
    family  = safe_col(authors_df, "family"),
    email   = email_col,
    role    = role_col,
    orcid   = safe_col(authors_df, "ORCID"),
    ror_id  = safe_col(authors_df, "ROR_ID"),
    stringsAsFactors = FALSE
  )

  dbBegin(con)
  dbWriteTable(con, "authors", write_df, append = TRUE)
  dbCommit(con)
  counts$authors <- nrow(write_df)
  cat("  Wrote", nrow(write_df), "author entries\n")
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 6. Package Enrichment
# =========================================================================
cat("\n=== 6. Package Enrichment ===\n")
pdb <- NULL
tryCatch({
  pdb <- tools::CRAN_package_db()
  # Deduplicate (recommended packages can appear twice)
  pdb <- pdb[!duplicated(pdb$Package), ]
  cat("  Fetched", nrow(pdb), "packages\n")

  dbExecute(con, "DROP TABLE IF EXISTS packages_enrichment")
  dbExecute(con, "
  CREATE TABLE packages_enrichment (
    name        TEXT PRIMARY KEY,
    url         TEXT,
    bug_reports TEXT
  )")

  write_df <- data.frame(
    name        = pdb$Package,
    url         = ifelse(is.na(pdb$URL), "", pdb$URL),
    bug_reports = ifelse(is.na(pdb$BugReports), "", pdb$BugReports),
    stringsAsFactors = FALSE
  )

  dbBegin(con)
  dbWriteTable(con, "packages_enrichment", write_df, append = TRUE)
  dbCommit(con)
  counts$enrichment <- nrow(write_df)
  cat("  Wrote", nrow(write_df), "enrichment entries\n")
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 7. Archival Reasons (sample of ERROR packages, limit 50)
# =========================================================================
cat("\n=== 7. Archival Reasons ===\n")
tryCatch({
  dbExecute(con, "DROP TABLE IF EXISTS removal_reasons")
  dbExecute(con, "
  CREATE TABLE removal_reasons (
    package TEXT PRIMARY KEY,
    reason  TEXT
  )")

  # Get packages with ERROR status from check results
  error_pkgs <- character(0)
  if (dbExistsTable(con, "cran_check_results")) {
    error_pkgs <- dbGetQuery(con, "
      SELECT DISTINCT package FROM cran_check_results WHERE status = 'ERROR'
    ")$package
  }

  if (length(error_pkgs) > 0) {
    # Sample up to 50
    sample_pkgs <- head(error_pkgs, 50)
    cat("  Checking", length(sample_pkgs), "packages with ERROR status\n")

    reason_pkg  <- character(0)
    reason_text <- character(0)

    for (pkg in sample_pkgs) {
      tryCatch({
        url <- sprintf("https://cran.r-project.org/web/packages/%s/index.html", pkg)
        page_lines <- readLines(url, warn = FALSE)
        page_text <- paste(page_lines, collapse = "\n")

        m <- regmatches(page_text,
                        regexpr("Archived on [0-9]{4}-[0-9]{2}-[0-9]{2}[^.]*\\.", page_text))
        if (length(m) > 0) {
          reason_match <- regmatches(m[1], regexpr("as .*\\.", m[1]))
          if (length(reason_match) > 0) {
            reason <- sub("^as ", "", reason_match[1])
            reason <- sub("\\.$", "", reason)
          } else {
            reason <- m[1]
          }
          reason_pkg  <- c(reason_pkg, pkg)
          reason_text <- c(reason_text, reason)
        }
      }, error = function(e) NULL, warning = function(w) NULL)
    }

    if (length(reason_pkg) > 0) {
      write_df <- data.frame(
        package = reason_pkg,
        reason  = reason_text,
        stringsAsFactors = FALSE
      )
      dbBegin(con)
      dbWriteTable(con, "removal_reasons", write_df, append = TRUE)
      dbCommit(con)
      counts$removal_reasons <- nrow(write_df)
      cat("  Found", nrow(write_df), "archival reasons\n")
    } else {
      cat("  No archival reasons found\n")
    }
  } else {
    cat("  No ERROR packages to check\n")
  }
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 8. Package NEWS (sample of 200 packages)
# =========================================================================
cat("\n=== 8. Package NEWS ===\n")
tryCatch({
  dbExecute(con, "DROP TABLE IF EXISTS package_news")
  dbExecute(con, "
  CREATE TABLE package_news (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    package   TEXT NOT NULL,
    version   TEXT NOT NULL,
    news_text TEXT
  )")
  dbExecute(con, "CREATE INDEX idx_pn_package ON package_news (package)")

  # Get a sample of packages (use enrichment data if available)
  sample_pkgs <- character(0)
  if (!is.null(pdb) && nrow(pdb) > 0) {
    all_pkgs <- pdb$Package
    sample_pkgs <- head(all_pkgs, 200)
  } else if (dbExistsTable(con, "packages_enrichment")) {
    sample_pkgs <- dbGetQuery(con, "SELECT name FROM packages_enrichment LIMIT 200")$name
  }

  if (length(sample_pkgs) > 0) {
    cat("  Fetching NEWS for", length(sample_pkgs), "packages\n")

    news_pkg     <- character(0)
    news_version <- character(0)
    news_text    <- character(0)

    for (pkg in sample_pkgs) {
      tryCatch({
        urls <- c(
          sprintf("https://raw.githubusercontent.com/cran/%s/master/NEWS.md", pkg),
          sprintf("https://raw.githubusercontent.com/cran/%s/master/NEWS", pkg)
        )
        content <- NULL
        for (url in urls) {
          tryCatch({
            content <- readLines(url, warn = FALSE, n = 500)
            break
          }, error = function(e) NULL, warning = function(w) NULL)
        }

        if (!is.null(content) && length(content) > 0) {
          full_text <- paste(content, collapse = "\n")

          # Extract version from CRAN package DB if available
          pkg_version <- NA_character_
          if (!is.null(pdb) && pkg %in% pdb$Package) {
            pkg_version <- pdb$Version[pdb$Package == pkg][1]
          }
          if (is.na(pkg_version)) pkg_version <- "latest"

          # Extract first version section (up to 2000 chars)
          # Look for headers like "# pkg 1.0.0" or "## Changes in version 1.0.0"
          header_pattern <- "(^|\\n)[#= ]+.*[0-9]+\\.[0-9]+"
          match_pos <- regexpr(header_pattern, full_text)
          if (match_pos > 0) {
            rest <- substring(full_text, match_pos)
            # Find next version header (skip first character to avoid matching same header)
            next_header <- regexpr("\\n[#= ]+.*[0-9]+\\.[0-9]+", substring(rest, 2))
            if (next_header > 0 && next_header < 2000) {
              section <- substring(rest, 1, next_header)
            } else {
              section <- substring(rest, 1, min(nchar(rest), 2000))
            }
            news_pkg     <- c(news_pkg, pkg)
            news_version <- c(news_version, pkg_version)
            news_text    <- c(news_text, trimws(section))
          }
        }
      }, error = function(e) NULL)
    }

    if (length(news_pkg) > 0) {
      write_df <- data.frame(
        package   = news_pkg,
        version   = news_version,
        news_text = news_text,
        stringsAsFactors = FALSE
      )
      dbBegin(con)
      dbWriteTable(con, "package_news", write_df, append = TRUE)
      dbCommit(con)
      counts$package_news <- nrow(write_df)
      cat("  Wrote", nrow(write_df), "NEWS entries\n")
    } else {
      cat("  No NEWS entries extracted\n")
    }
  } else {
    cat("  No packages available for NEWS fetch\n")
  }
}, error = function(e) {
  cat("  ERROR:", e$message, "\n")
  tryCatch(dbRollback(con), error = function(e2) NULL)
})

# =========================================================================
# 9. Release Notes
# =========================================================================
cat("\n=== 9. Release Notes ===\n")

db_size_bytes <- file.info(db_path)$size
if (db_size_bytes >= 1024 * 1024) {
  db_size <- sprintf("%.1f MB", db_size_bytes / (1024 * 1024))
} else {
  db_size <- sprintf("%.1f KB", db_size_bytes / 1024)
}

history_total <- tryCatch(
  dbGetQuery(con, "SELECT COUNT(*) AS n FROM check_status_history")$n,
  error = function(e) 0L
)

notes <- paste0(
  "## CRAN Metadata Update\n\n",
  "**", format(Sys.time(), "%Y-%m-%d %H:%M UTC", tz = "UTC"), "**\n\n",
  "| Table | Rows |\n",
  "|-------|------|\n",
  "| cran_check_results | ", counts$check_results, " |\n",
  "| cran_check_details | ", counts$check_details, " |\n",
  "| cran_check_issues | ", counts$check_issues, " |\n",
  "| check_status_history (new) | ", counts$history_changes, " |\n",
  "| check_status_history (total) | ", history_total, " |\n",
  "| authors | ", counts$authors, " |\n",
  "| packages_enrichment | ", counts$enrichment, " |\n",
  "| removal_reasons | ", counts$removal_reasons, " |\n",
  "| package_news | ", counts$package_news, " |\n",
  "| **Database size** | ", db_size, " |\n"
)

writeLines(notes, "release_notes.md")
cat("Wrote release_notes.md\n")

cat("\nDone.\n")
