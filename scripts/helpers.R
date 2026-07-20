# scripts/helpers.R: integrity / completeness manifest helpers for cran-metadata.

#' Compute the lowercase hex SHA-256 of a file's exact on-disk bytes.
#'
#' Uses whatever the runner already provides, in preference order:
#'   1. digest  package        (if installed)
#'   2. openssl package        (if installed)
#'   3. sha256sum (coreutils)  - present on the ubuntu-latest CI runner
#'   4. shasum -a 256 (BSD)    - macOS/local fallback
#' No heavy dependency is declared: on CI (which installs RSQLite, jsonlite,
#' testthat, withr) the coreutils `sha256sum` path is used. If a sibling
#' pipeline already declares `digest`, that path wins automatically.
file_sha256 <- function(path) {
  if (requireNamespace("digest", quietly = TRUE)) {
    return(tolower(digest::digest(file = path, algo = "sha256")))
  }
  if (requireNamespace("openssl", quietly = TRUE)) {
    con <- file(path, open = "rb")
    on.exit(close(con), add = TRUE)
    return(tolower(as.character(openssl::sha256(con))))
  }
  sha_tool <- Sys.which("sha256sum")
  if (nzchar(sha_tool)) {
    out <- system2(sha_tool, shQuote(path), stdout = TRUE)
    return(tolower(sub("\\s.*$", "", out[1])))
  }
  shasum_tool <- Sys.which("shasum")
  if (nzchar(shasum_tool)) {
    out <- system2(shasum_tool, c("-a", "256", shQuote(path)), stdout = TRUE)
    return(tolower(sub("\\s.*$", "", out[1])))
  }
  stop("No SHA-256 backend found (need one of: digest, openssl, sha256sum, shasum)")
}

#' Build the integrity / completeness core describing a finalized SQLite file.
#'
#' Returns a named list of TOP-LEVEL manifest fields computed from the exact
#' on-disk bytes of `db_path` (call this only after the file is finalized and
#' its DB connection closed, so any WAL is checkpointed into the main file):
#'   * db_filename - basename of the file
#'   * db_bytes    - byte size of the file as a double. Deliberately NOT cast
#'                   to integer: R's integer range is 32-bit and overflows to
#'                   NA (serialized as the string "NA") for files >= ~2 GiB.
#'   * db_sha256   - lowercase hex sha256 of the file's exact bytes
#'   * tables      - named list mapping each user table to its row count
#'   * complete    - passed through by the caller. complete = the DB holds the
#'                   full, non-partial dataset (full-not-partial), NOT freshness:
#'                   freshness is tracked separately via generated_at and the
#'                   db_sha256 fingerprint. A pipeline with a genuine
#'                   partial/bootstrap state DERIVES this instead of hardcoding.
#' Lets a downstream merge content-verify the asset it pulls and confirm the
#' expected tables/rows are present.
summary_integrity_core <- function(db_path, complete) {
  stopifnot(file.exists(db_path))

  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  tables <- tryCatch({
    tbl_names <- DBI::dbGetQuery(con, "
      SELECT name FROM sqlite_master
       WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
       ORDER BY name")$name

    stats::setNames(
      lapply(tbl_names, function(t) {
        DBI::dbGetQuery(con, sprintf('SELECT count(*) AS n FROM "%s"', t))$n
      }),
      tbl_names
    )
  }, finally = DBI::dbDisconnect(con))

  # db_bytes/db_sha256 read the raw on-disk file only after the connection
  # above is closed, so no open handle or journal file skews the hash/size.
  list(
    db_filename = basename(db_path),
    db_bytes    = file.size(db_path),
    db_sha256   = file_sha256(db_path),
    tables      = tables,
    complete    = complete
  )
}

#' Write the release manifest.json describing the finalized primary DB.
#'
#' Top-level fields: generated_at plus the integrity/completeness core produced
#' by summary_integrity_core(). `core` is merged as TOP-LEVEL fields (not nested)
#' so a downstream merge can read db_filename/db_bytes/db_sha256/tables/complete
#' directly. generated_at records freshness independently of `complete`.
write_manifest <- function(path, core,
                           generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ",
                                                 tz = "UTC")) {
  obj <- c(list(generated_at = generated_at), core)
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, pretty = TRUE, null = "null")
  writeLines(json, path)
  invisible(path)
}

#' Decide whether today's deadline snapshot is trustworthy enough to diff.
#'
#' A missing column, an empty snapshot against a non-empty prior open set, or a
#' single-run drop in the open set beyond `drop_frac_max` all signal a bad input
#' (e.g. CRAN renamed/removed the undocumented Deadline column, or a partial
#' fetch). In those cases the caller skips the diff and preserves prior rows,
#' rather than mass-closing every open episode as "met". The mass-disappearance
#' guards only make sense at scale: below `min_prior` open episodes, a snapshot
#' dropping (even to 0) is ordinary churn, not a broken fetch, so the diff
#' proceeds.
deadline_snapshot_healthy <- function(snapshot_n, prior_open_n, has_col,
                                      drop_frac_max = 0.5, min_prior = 20L) {
  if (!isTRUE(has_col)) return(FALSE)
  if (prior_open_n < min_prior) return(TRUE)
  if (snapshot_n == 0L) return(FALSE)
  if ((prior_open_n - snapshot_n) / prior_open_n > drop_frac_max) return(FALSE)
  TRUE
}

#' Diff today's non-NA Deadline snapshot against the prior open episodes.
#'
#' Returns `inserts` (new open episodes, all ten columns) and `updates` (one row
#' per changed existing episode, columns deadline/last_seen/resolved_on/outcome/
#' package/episode_seq). The open marker is resolved_on IS NULL; outcome is only
#' ever NA, "met", or "vanished" here (the viewer enrich upgrades to "archived").
compute_deadline_changes <- function(prior_open, snapshot, current_packages,
                                     worst_status_map, max_seq_map, today) {
  open_pkgs  <- prior_open$package
  snap_pkgs  <- snapshot$package
  cur        <- unique(current_packages)

  ins <- data.frame(package=character(0), episode_seq=integer(0), deadline=character(0),
    version=character(0), worst_status=character(0), first_seen=character(0),
    last_seen=character(0), resolved_on=character(0), outcome=character(0),
    archived_on=character(0), stringsAsFactors=FALSE)
  upd <- data.frame(deadline=character(0), last_seen=character(0), resolved_on=character(0),
    outcome=character(0), package=character(0), episode_seq=integer(0), stringsAsFactors=FALSE)

  # 1. Packages with a deadline today.
  for (i in seq_along(snap_pkgs)) {
    p <- snap_pkgs[i]; d <- snapshot$deadline[i]; v <- snapshot$version[i]
    j <- match(p, open_pkgs)
    if (!is.na(j)) {
      # re-observed: extend last_seen and the (possibly changed) deadline; stay open
      upd[nrow(upd) + 1L, ] <- list(d, today, NA_character_, NA_character_, p, prior_open$episode_seq[j])
    } else {
      # brand-new open episode
      seq_next <- if (!is.na(max_seq_map[p])) as.integer(max_seq_map[p]) + 1L else 1L
      ws <- if (!is.na(worst_status_map[p])) unname(worst_status_map[p]) else NA_character_
      ins[nrow(ins) + 1L, ] <- list(p, seq_next, d, v, ws, today, today,
                                    NA_character_, NA_character_, NA_character_)
    }
  }

  # 2. Prior open episodes with no deadline today -> close.
  gone <- setdiff(open_pkgs, snap_pkgs)
  for (p in gone) {
    j <- match(p, open_pkgs)
    outcome <- if (p %in% cur) "met" else "vanished"
    # last_seen and deadline unchanged (not observed today); only mark resolved.
    upd[nrow(upd) + 1L, ] <- list(prior_open$deadline[j], prior_open$last_seen[j],
                                  today, outcome, p, prior_open$episode_seq[j])
  }

  list(inserts = ins, updates = upd)
}

#' Build/refresh the cran_check_deadlines episode table on a connected metadata.db.
#'
#' Ensures the schema (persistent, never dropped), reads the prior open episodes
#' and per-package max episode_seq from the connected (downloaded) DB, diffs
#' today's tools::CRAN_package_db()$Deadline snapshot, and applies inserts +
#' updates in one transaction. Skips the diff (preserving prior rows) when the
#' snapshot fails the no-data floor. `worst_status_map` is the same in-memory
#' worst-check-status vector update.R computes for check_status_history.
write_deadlines <- function(con, pdb,
                            worst_status_map = setNames(character(0), character(0)),
                            today = as.character(Sys.Date()),
                            drop_frac_max = 0.5) {
  DBI::dbExecute(con, "CREATE TABLE IF NOT EXISTS cran_check_deadlines (
    package TEXT NOT NULL, episode_seq INTEGER NOT NULL, deadline TEXT NOT NULL,
    version TEXT, worst_status TEXT, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
    resolved_on TEXT, outcome TEXT, archived_on TEXT,
    PRIMARY KEY (package, episode_seq),
    CHECK (resolved_on IS NULL OR resolved_on <> ''),
    CHECK ((resolved_on IS NULL) = (outcome IS NULL)),
    CHECK (last_seen >= first_seen))")
  DBI::dbExecute(con, "CREATE UNIQUE INDEX IF NOT EXISTS ux_cran_check_deadlines_open
    ON cran_check_deadlines(package) WHERE resolved_on IS NULL")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_ccd_open_deadline
    ON cran_check_deadlines(deadline) WHERE resolved_on IS NULL")

  prior_open <- DBI::dbGetQuery(con,
    "SELECT package, episode_seq, deadline, last_seen
       FROM cran_check_deadlines WHERE resolved_on IS NULL")
  ms <- DBI::dbGetQuery(con,
    "SELECT package, MAX(episode_seq) AS max_seq FROM cran_check_deadlines GROUP BY package")
  max_seq_map <- if (nrow(ms)) setNames(ms$max_seq, ms$package) else setNames(integer(0), character(0))

  has_col <- is.data.frame(pdb) && "Deadline" %in% names(pdb)
  if (has_col) {
    keep <- !is.na(pdb$Deadline) & nzchar(pdb$Deadline)
    snapshot <- data.frame(
      package = pdb$Package[keep],
      deadline = pdb$Deadline[keep],
      version = if ("Version" %in% names(pdb)) pdb$Version[keep] else NA_character_,
      stringsAsFactors = FALSE)
  } else {
    snapshot <- data.frame(package=character(0), deadline=character(0),
                           version=character(0), stringsAsFactors=FALSE)
  }

  # CRAN_package_db() can contain duplicate package rows; keep the first so the
  # open-episode unique index never trips even if the caller did not dedup pdb.
  snapshot <- snapshot[!duplicated(snapshot$package), , drop = FALSE]

  current_packages <- if (is.data.frame(pdb) && "Package" %in% names(pdb)) pdb$Package else character(0)

  if (!deadline_snapshot_healthy(nrow(snapshot), nrow(prior_open), has_col, drop_frac_max)) {
    return(list(skipped = TRUE, new = 0L, extended = 0L, closed = 0L))
  }

  ch <- compute_deadline_changes(prior_open, snapshot, current_packages,
                                 worst_status_map, max_seq_map, today)

  DBI::dbBegin(con)
  ok <- FALSE
  on.exit(if (!ok) tryCatch(DBI::dbRollback(con), error = function(e) NULL), add = TRUE)
  if (nrow(ch$inserts) > 0) DBI::dbWriteTable(con, "cran_check_deadlines", ch$inserts, append = TRUE)
  if (nrow(ch$updates) > 0) {
    DBI::dbExecute(con,
      "UPDATE cran_check_deadlines SET deadline = ?, last_seen = ?, resolved_on = ?, outcome = ?
        WHERE package = ? AND episode_seq = ?",
      params = list(ch$updates$deadline, ch$updates$last_seen, ch$updates$resolved_on,
                    ch$updates$outcome, ch$updates$package, ch$updates$episode_seq))
  }
  DBI::dbCommit(con); ok <- TRUE

  extended <- sum(is.na(ch$updates$outcome))
  closed   <- sum(!is.na(ch$updates$outcome))
  list(skipped = FALSE, new = nrow(ch$inserts), extended = extended, closed = closed)
}
