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

invisible(dbExecute(con, "PRAGMA journal_mode=WAL"))
invisible(dbExecute(con, "PRAGMA synchronous=NORMAL"))

# ---------------------------------------------------------------------------
# Text sanitization — CRAN data can contain NUL bytes, other control
# characters, and non-UTF-8 encodings that break SQLite or downstream JSON.
# Uses Perl \\x{00} syntax instead of literal \x00 escapes to avoid
# embedding actual NUL bytes in the source file (which crashes R's parser).
# ---------------------------------------------------------------------------
sanitize_df <- function(df) {
  for (col in names(df)) {
    if (is.character(df[[col]])) {
      # Strip NUL and other problematic control chars in one pass (keep \n \r \t)
      df[[col]] <- gsub("[\\x{00}-\\x{08}\\x{0b}\\x{0c}\\x{0e}-\\x{1f}]", "",
                         df[[col]], perl = TRUE)
      # Force valid UTF-8 (drop unrepresentable bytes)
      df[[col]] <- iconv(df[[col]], to = "UTF-8", sub = "")
    }
  }
  df
}

# ---------------------------------------------------------------------------
# Extract diagnostic signal from CRAN check output, discarding build noise.
# Returns the error/warning messages without compiler invocation lines,
# make directory changes, or installation boilerplate.
# ---------------------------------------------------------------------------
extract_check_signal <- function(output, max_chars = 2000L) {
  if (is.na(output) || !nzchar(output)) return(NA_character_)

  lines <- strsplit(output, "\n", fixed = TRUE)[[1L]]

  # Drop compiler invocation lines (gcc/g++/clang/gfortran with flags)
  noise <- grepl("^\\s*(gcc|g\\+\\+|clang|cc|c\\+\\+|gfortran)\\s+-", lines)
  # Drop make directory changes
  noise <- noise | grepl("^make\\[\\d+\\]: (Entering|Leaving) directory", lines)
  # Drop staged installation / byte-compile / removing temp dir boilerplate
  noise <- noise | grepl("^\\*\\* (using staged|made |byte-compil)", lines)
  noise <- noise | grepl("^\\* removing '/", lines)
  # Drop linker invocation lines
  noise <- noise | grepl("^\\s*(ld|ar|ranlib|install_name_tool)\\s+", lines)

  kept <- lines[!noise]
  result <- paste(kept, collapse = "\n")
  result <- trimws(result)

  if (nchar(result) > max_chars) {
    # Keep first + last portions: beginning has context, end has the error
    half <- max_chars %/% 2L
    result <- paste0(
      substr(result, 1L, half),
      "\n...[truncated]...\n",
      substring(result, nchar(result) - half + 1L)
    )
  }
  result
}

# Vectorized wrapper for extract_check_signal
extract_check_signal_vec <- function(outputs, max_chars = 2000L) {
  vapply(outputs, extract_check_signal, character(1L),
         max_chars = max_chars, USE.NAMES = FALSE)
}

# ---------------------------------------------------------------------------
# JSON escape helper
# ---------------------------------------------------------------------------
json_escape <- function(s) {
  s <- gsub("\\\\", "\\\\\\\\", s)
  s <- gsub('"', '\\\\"', s)
  s <- gsub("\n", "\\\\n", s)
  s <- gsub("\t", "\\\\t", s)
  s <- gsub("\r", "\\\\r", s)
  s <- gsub("[\\x{00}-\\x{08}\\x{0b}\\x{0c}\\x{0e}-\\x{1f}]", "", s, perl = TRUE)
  s
}

# ---------------------------------------------------------------------------
# Create append-only table (never dropped)
# ---------------------------------------------------------------------------
invisible(dbExecute(con, "
CREATE TABLE IF NOT EXISTS check_status_history (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  package         TEXT NOT NULL,
  status          TEXT NOT NULL,
  flavor_summary  TEXT,
  details         TEXT,
  detected_at     TEXT NOT NULL
)"))
invisible(dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_csh_package  ON check_status_history (package)"))
invisible(dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_csh_detected ON check_status_history (detected_at)"))

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

  invisible(dbExecute(con, "DROP TABLE IF EXISTS cran_check_results"))
  invisible(dbExecute(con, "
  CREATE TABLE cran_check_results (
    package  TEXT NOT NULL,
    flavor   TEXT NOT NULL,
    status   TEXT NOT NULL,
    tinstall REAL,
    tcheck   REAL,
    ttotal   REAL,
    PRIMARY KEY (package, flavor)
  )"))
  invisible(dbExecute(con, "CREATE INDEX idx_ccr_status ON cran_check_results (status)"))

  write_df <- data.frame(
    package  = results_df$Package,
    flavor   = results_df$Flavor,
    status   = results_df$Status,
    tinstall = as.numeric(results_df$T_install),
    tcheck   = as.numeric(results_df$T_check),
    ttotal   = as.numeric(results_df$T_total),
    stringsAsFactors = FALSE
  )
  write_df <- write_df[!is.na(write_df$package) & !is.na(write_df$flavor) & !is.na(write_df$status), ]
  cat("  After filtering NAs:", nrow(write_df), "rows\n")

  write_df <- sanitize_df(write_df)
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
# 2. Check Details (non-OK only, with signal extraction)
# =========================================================================
cat("\n=== 2. CRAN Check Details ===\n")
check_details_df <- NULL
tryCatch({
  check_details_df <- as.data.frame(tools::CRAN_check_details())
  cat("  Fetched", nrow(check_details_df), "rows\n")

  # Only store non-OK results — OK rows carry no diagnostic value
  check_details_df <- check_details_df[
    !is.na(check_details_df$Status) & check_details_df$Status != "OK", ]
  cat("  Non-OK rows:", nrow(check_details_df), "\n")

  invisible(dbExecute(con, "DROP TABLE IF EXISTS cran_check_details"))
  invisible(dbExecute(con, "
  CREATE TABLE cran_check_details (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    package    TEXT NOT NULL,
    flavor     TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status     TEXT NOT NULL,
    output     TEXT
  )"))
  invisible(dbExecute(con, "CREATE INDEX idx_ccd_package ON cran_check_details (package)"))
  invisible(dbExecute(con, "CREATE INDEX idx_ccd_status  ON cran_check_details (status)"))

  write_df <- data.frame(
    package    = check_details_df$Package,
    flavor     = check_details_df$Flavor,
    check_name = if ("Check" %in% names(check_details_df)) check_details_df$Check else NA_character_,
    status     = check_details_df$Status,
    output     = if ("Output" %in% names(check_details_df))
                   extract_check_signal_vec(check_details_df$Output) else NA_character_,
    stringsAsFactors = FALSE
  )
  write_df <- write_df[!is.na(write_df$package) & !is.na(write_df$check_name) & !is.na(write_df$status), ]
  cat("  After filtering NAs:", nrow(write_df), "rows\n")

  write_df <- sanitize_df(write_df)
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
    results_clean <- results_df[!is.na(results_df$Package) & !is.na(results_df$Status), ]
    cat("  Using", nrow(results_clean), "clean check result rows for history\n")

    status_levels <- c("OK" = 0L, "NOTE" = 1L, "WARNING" = 2L, "ERROR" = 3L)

    pkg_list <- unique(results_clean$Package)
    n_pkgs <- length(pkg_list)

    # Vectorised worst-status computation using split
    pkg_factor <- factor(results_clean$Package, levels = pkg_list)
    severity_vec <- ifelse(results_clean$Status %in% names(status_levels),
                           status_levels[results_clean$Status], -1L)

    worst_sev <- tapply(severity_vec, pkg_factor, max)
    worst_sev <- pmax(worst_sev, 0L)
    sev_to_status <- c("OK", "NOTE", "WARNING", "ERROR")
    worst_status <- sev_to_status[worst_sev + 1L]

    # Flavor summary: count of each status per package as JSON
    flavor_summary <- character(n_pkgs)
    status_split <- split(results_clean$Status, pkg_factor)
    for (i in seq_along(pkg_list)) {
      tbl <- table(status_split[[i]])
      pairs <- paste0('"', names(tbl), '":', as.integer(tbl))
      flavor_summary[i] <- paste0("{", paste(pairs, collapse = ","), "}")
    }

    # Pre-split for O(N+M) lookup
    results_by_pkg <- split(results_clean, results_clean$Package)
    if (!is.null(check_details_df) && nrow(check_details_df) > 0) {
      details_by_pkg <- split(check_details_df, check_details_df$Package)
    } else {
      details_by_pkg <- list()
    }

    # Details: JSON array of non-OK entries with extracted signal
    details_json <- character(n_pkgs)
    for (i in seq_along(pkg_list)) {
      pkg <- pkg_list[i]
      pkg_rows <- results_by_pkg[[pkg]]
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
        pkg_details <- details_by_pkg[[pkg]]
        if (!is.null(pkg_details) && nrow(pkg_details) > 0) {
          match_rows <- pkg_details[pkg_details$Flavor == flav, , drop = FALSE]
          if (nrow(match_rows) > 0) {
            if ("Check" %in% names(match_rows)) chk <- as.character(match_rows$Check[1])
            if ("Output" %in% names(match_rows)) {
              out_raw <- match_rows$Output[1]
              if (!is.na(out_raw)) out <- extract_check_signal(out_raw, max_chars = 500L)
            }
          }
        }
        if (is.na(out)) out <- ""
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
      if (is.na(prev_map[pkg])) return(TRUE)
      prev_map[pkg] != worst_status[i]
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

      changes_df <- sanitize_df(changes_df)
      dbBegin(con)
      dbWriteTable(con, "check_status_history", changes_df, append = TRUE)
      dbCommit(con)

      counts$history_changes <- nrow(changes_df)
      cat("  Inserted", nrow(changes_df), "status changes\n")
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

  invisible(dbExecute(con, "DROP TABLE IF EXISTS cran_check_issues"))
  invisible(dbExecute(con, "
  CREATE TABLE cran_check_issues (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL,
    version TEXT,
    kind    TEXT NOT NULL,
    href    TEXT
  )"))
  invisible(dbExecute(con, "CREATE INDEX idx_cci_package ON cran_check_issues (package)"))

  # Map columns safely — names may vary across R versions
  safe_issues_col <- function(df, candidates) {
    for (col in candidates) {
      if (col %in% names(df)) return(as.character(df[[col]]))
    }
    rep(NA_character_, nrow(df))
  }

  write_df <- data.frame(
    package = safe_issues_col(issues, c("Package", "package")),
    version = safe_issues_col(issues, c("Version", "version")),
    kind    = safe_issues_col(issues, c("Kind", "kind")),
    href    = safe_issues_col(issues, c("href", "Href", "URL", "url")),
    stringsAsFactors = FALSE
  )
  write_df <- write_df[!is.na(write_df$package) & !is.na(write_df$kind), ]

  write_df <- sanitize_df(write_df)
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

  invisible(dbExecute(con, "DROP TABLE IF EXISTS authors"))
  invisible(dbExecute(con, "
  CREATE TABLE authors (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL,
    given   TEXT,
    family  TEXT,
    email   TEXT,
    role    TEXT,
    orcid   TEXT,
    ror_id  TEXT
  )"))
  invisible(dbExecute(con, "CREATE INDEX idx_authors_package ON authors (package)"))
  invisible(dbExecute(con, "CREATE INDEX idx_authors_name    ON authors (family, given)"))

  # Build write data — handle columns that may not exist
  safe_col <- function(df, candidates) {
    for (col in candidates) {
      if (col %in% names(df)) {
        vals <- df[[col]]
        if (is.list(vals)) {
          return(vapply(vals, function(v) {
            if (is.null(v) || all(is.na(v))) NA_character_
            else paste(as.character(v), collapse = ", ")
          }, character(1)))
        }
        return(as.character(vals))
      }
    }
    rep(NA_character_, nrow(df))
  }

  write_df <- data.frame(
    package = safe_col(authors_df, c("Package", "package")),
    given   = safe_col(authors_df, c("given", "Given")),
    family  = safe_col(authors_df, c("family", "Family")),
    email   = safe_col(authors_df, c("email", "Email")),
    role    = safe_col(authors_df, c("role", "Role")),
    orcid   = safe_col(authors_df, c("ORCID", "orcid")),
    ror_id  = safe_col(authors_df, c("ROR_ID", "ror_id", "ROR")),
    stringsAsFactors = FALSE
  )
  write_df <- write_df[!is.na(write_df$package), ]
  cat("  After filtering:", nrow(write_df), "rows\n")

  write_df <- sanitize_df(write_df)
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
  pdb <- pdb[!duplicated(pdb$Package), ]
  cat("  Fetched", nrow(pdb), "packages\n")

  invisible(dbExecute(con, "DROP TABLE IF EXISTS packages_enrichment"))
  invisible(dbExecute(con, "
  CREATE TABLE packages_enrichment (
    name        TEXT PRIMARY KEY,
    url         TEXT,
    bug_reports TEXT
  )"))

  write_df <- data.frame(
    name        = pdb$Package,
    url         = ifelse(is.na(pdb$URL), "", pdb$URL),
    bug_reports = ifelse(is.na(pdb$BugReports), "", pdb$BugReports),
    stringsAsFactors = FALSE
  )

  write_df <- sanitize_df(write_df)
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
  invisible(dbExecute(con, "DROP TABLE IF EXISTS removal_reasons"))
  invisible(dbExecute(con, "
  CREATE TABLE removal_reasons (
    package TEXT PRIMARY KEY,
    reason  TEXT
  )"))

  error_pkgs <- character(0)
  if (dbExistsTable(con, "cran_check_results")) {
    error_pkgs <- dbGetQuery(con, "
      SELECT DISTINCT package FROM cran_check_results WHERE status = 'ERROR'
    ")$package
  }

  if (length(error_pkgs) > 0) {
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
      write_df <- sanitize_df(write_df)
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
  invisible(dbExecute(con, "DROP TABLE IF EXISTS package_news"))
  invisible(dbExecute(con, "
  CREATE TABLE package_news (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    package   TEXT NOT NULL,
    version   TEXT NOT NULL,
    news_text TEXT
  )"))
  invisible(dbExecute(con, "CREATE INDEX idx_pn_package ON package_news (package)"))

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

          pkg_version <- NA_character_
          if (!is.null(pdb) && pkg %in% pdb$Package) {
            pkg_version <- pdb$Version[pdb$Package == pkg][1]
          }
          if (is.na(pkg_version)) pkg_version <- "latest"

          # Extract first version section (up to 2000 chars)
          header_pattern <- "(^|\\n)[#= ]+.*[0-9]+\\.[0-9]+"
          match_pos <- regexpr(header_pattern, full_text)
          if (match_pos > 0) {
            rest <- substring(full_text, match_pos)
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
      write_df <- sanitize_df(write_df)
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
