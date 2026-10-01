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
#' Episodes opened by the run that creates the table get onset_known = 0, and
#' an unhealthy first snapshot creates nothing, so that stays true.
write_deadlines <- function(con, pdb,
                            worst_status_map = setNames(character(0), character(0)),
                            today = as.character(Sys.Date()),
                            drop_frac_max = 0.5) {
  existed <- DBI::dbExistsTable(con, "cran_check_deadlines")
  if (existed) {
    prior_open <- DBI::dbGetQuery(con,
      "SELECT package, episode_seq, deadline, last_seen
         FROM cran_check_deadlines WHERE resolved_on IS NULL")
    ms <- DBI::dbGetQuery(con,
      "SELECT package, MAX(episode_seq) AS max_seq FROM cran_check_deadlines GROUP BY package")
  } else {
    prior_open <- data.frame(package = character(0), episode_seq = integer(0),
                             deadline = character(0), last_seen = character(0),
                             stringsAsFactors = FALSE)
    ms <- data.frame(package = character(0), max_seq = integer(0), stringsAsFactors = FALSE)
  }
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

  DBI::dbExecute(con, "CREATE TABLE IF NOT EXISTS cran_check_deadlines (
    package TEXT NOT NULL, episode_seq INTEGER NOT NULL, deadline TEXT NOT NULL,
    version TEXT, worst_status TEXT, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
    resolved_on TEXT, outcome TEXT, archived_on TEXT, onset_known INTEGER,
    PRIMARY KEY (package, episode_seq),
    CHECK (resolved_on IS NULL OR resolved_on <> ''),
    CHECK ((resolved_on IS NULL) = (outcome IS NULL)),
    CHECK (last_seen >= first_seen))")
  # Rows from before the column existed keep NULL: their onset was not recorded.
  if (!"onset_known" %in% DBI::dbListFields(con, "cran_check_deadlines")) {
    DBI::dbExecute(con, "ALTER TABLE cran_check_deadlines ADD COLUMN onset_known INTEGER")
  }
  DBI::dbExecute(con, "CREATE UNIQUE INDEX IF NOT EXISTS ux_cran_check_deadlines_open
    ON cran_check_deadlines(package) WHERE resolved_on IS NULL")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_ccd_open_deadline
    ON cran_check_deadlines(deadline) WHERE resolved_on IS NULL")

  ch <- compute_deadline_changes(prior_open, snapshot, current_packages,
                                 worst_status_map, max_seq_map, today)
  ch$inserts$onset_known <- rep(if (existed) 1L else 0L, nrow(ch$inserts))

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

# An ORCID iD not glued to further digits, and a ROR id as it follows ror.org/.
ORCID_ID_PATTERN <- "(?<![0-9])[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X](?![0-9X])"
ROR_ID_PATTERN   <- "0[a-hj-km-np-tv-z0-9]{6}[0-9]{2}"

# ISO 7064 MOD 11-2, the check ORCID defines, so a mistyped iD never moves.
orcid_checksum_ok <- function(id) {
  vapply(id, function(one) {
    if (is.na(one)) return(FALSE)
    digits <- gsub("-", "", one, fixed = TRUE)
    if (!grepl("^[0-9]{15}[0-9X]$", digits)) return(FALSE)
    total <- 0
    for (d in as.integer(strsplit(substr(digits, 1, 15), "")[[1]])) total <- (total + d) * 2
    r <- (12 - total %% 11) %% 11
    identical(if (r == 10) "X" else as.character(r), substr(digits, 16, 16))
  }, logical(1), USE.NAMES = FALSE)
}

# One line per comment; a blank comment is stored as NULL, never as "".
collapse_comment_whitespace <- function(x) {
  x <- trimws(gsub("[[:space:]]+", " ", x, perl = TRUE))
  x[!is.na(x) & !nzchar(x)] <- NA_character_
  x
}

# Never truncates: the viewer reads review links from the full text. A comment
# empties only when a moved identifier and its label were all it held.
normalize_author_comments <- function(comment, orcid, ror_id) {
  comment <- collapse_comment_whitespace(as.character(comment))
  orcid   <- as.character(orcid)
  ror_id  <- as.character(ror_id)
  absent  <- function(v) is.na(v) || !nzchar(v)
  orcid_hits <- regmatches(comment, gregexpr(ORCID_ID_PATTERN, comment, perl = TRUE))
  ror_hits   <- regmatches(comment, gregexpr(
    paste0("(?<![a-z0-9-])ror\\.org/", ROR_ID_PATTERN, "(?![a-z0-9])"), comment, perl = TRUE))
  n_orcid <- 0L
  n_ror   <- 0L
  for (i in which(!is.na(comment))) {
    rest  <- comment[i]
    moved <- FALSE
    # The same iD written twice is still one iD.
    id <- unique(orcid_hits[[i]])
    if (absent(orcid[i]) && length(id) == 1L && orcid_checksum_ok(id)) {
      orcid[i] <- id
      n_orcid  <- n_orcid + 1L
      moved    <- TRUE
      rest <- gsub(paste0("(?i)(orcid(\\s*id)?\\s*[:=]?\\s*)?[\"'<]?((https?://)?(www\\.)?orcid\\.org/)?",
                          id, "[\"'>]?"), "", rest, perl = TRUE)
    }
    id <- unique(sub("^ror\\.org/", "", ror_hits[[i]]))
    if (absent(ror_id[i]) && length(id) == 1L) {
      ror_id[i] <- id
      n_ror     <- n_ror + 1L
      moved     <- TRUE
      rest <- gsub(paste0("(?i)(ror(\\s*id)?\\s*[:=]?\\s*)?[\"'<]?(https?://)?(www\\.)?ror\\.org/",
                          id, "[\"'>]?"), "", rest, perl = TRUE)
    }
    if (moved && grepl("^[[:punct:][:space:]]*$", rest)) comment[i] <- NA_character_
  }
  list(comment = comment, orcid = orcid, ror_id = ror_id,
       n_orcid = n_orcid, n_ror = n_ror)
}

# Control characters other than tab, LF and CR break SQLite and downstream JSON.
# The \\x{00} form keeps literal NUL bytes out of this file, which R cannot parse.
sanitize_df <- function(df) {
  for (col in names(df)) {
    if (is.character(df[[col]])) {
      df[[col]] <- gsub("[\\x{00}-\\x{08}\\x{0b}\\x{0c}\\x{0e}-\\x{1f}]", "",
                         df[[col]], perl = TRUE)
      df[[col]] <- iconv(df[[col]], to = "UTF-8", sub = "")
    }
  }
  df
}

# CRAN gives some ROR ids as https://ror.org/<id>. Only a value that reduces to
# the bare id changes; anything else is returned as given.
normalize_ror_id <- function(x) {
  x <- as.character(x)
  bare <- sub("/$", "", sub("^(?i)(https?://)?(www\\.)?ror\\.org/", "", trimws(x), perl = TRUE))
  ok <- !is.na(x) & grepl(paste0("^", ROR_ID_PATTERN, "$"), bare, perl = TRUE)
  x[ok] <- bare[ok]
  x
}

# An error while sanitizing or normalizing comments empties only comment, so the
# other columns still land. `normalize` is replaceable so tests can force it.
build_authors_df <- function(authors_df, normalize = normalize_author_comments) {
  # Column names and list-vs-character types vary across R versions.
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

  out <- data.frame(
    package = safe_col(authors_df, c("Package", "package")),
    given   = safe_col(authors_df, c("given", "Given")),
    family  = safe_col(authors_df, c("family", "Family")),
    email   = safe_col(authors_df, c("email", "Email")),
    role    = safe_col(authors_df, c("role", "Role")),
    orcid   = safe_col(authors_df, c("ORCID", "orcid")),
    ror_id  = safe_col(authors_df, c("ROR_ID", "ror_id", "ROR")),
    comment = safe_col(authors_df, c("comment", "Comment")),
    stringsAsFactors = FALSE
  )
  out <- out[!is.na(out$package), , drop = FALSE]
  rownames(out) <- NULL
  others <- setdiff(names(out), "comment")
  out[others] <- sanitize_df(out[others])

  given_ror  <- out$ror_id
  out$ror_id <- normalize_ror_id(given_ror)
  n_reduced  <- sum(!is.na(given_ror) & given_ror != out$ror_id)
  odd <- which(!is.na(out$ror_id) & nzchar(out$ror_id) &
               !grepl(paste0("^", ROR_ID_PATTERN, "$"), out$ror_id, perl = TRUE))
  if (length(odd) > 0) {
    shown <- head(odd, 10)
    cat("  WARN:", length(odd), if (length(odd) == 1L) "ror_id value" else "ror_id values",
        "kept as given, not a ROR id:",
        paste0(out$package[shown], ": ", out$ror_id[shown], collapse = "; "),
        if (length(odd) > length(shown)) "; ..." else "", "\n")
  }

  fixed <- tryCatch({
    res <- normalize(sanitize_df(out["comment"])$comment, out$orcid, out$ror_id)
    n <- nrow(out)
    stopifnot(length(res$comment) == n, length(res$orcid) == n,
              length(res$ror_id) == n)
    res
  }, error = function(e) {
    cat("  WARN: author comments left empty after a normalization error:",
        conditionMessage(e), "\n")
    NULL
  })

  if (is.null(fixed)) {
    out$comment <- rep(NA_character_, nrow(out))
    attr(out, "recovered") <- c(orcid = 0L, ror = 0L)
  } else {
    out$comment <- fixed$comment
    out$orcid   <- fixed$orcid
    out$ror_id  <- fixed$ror_id
    attr(out, "recovered") <- c(orcid = fixed$n_orcid, ror = fixed$n_ror)
  }
  attr(out, "ror_ids_reduced") <- n_reduced
  out
}

# The table is rebuilt from CRAN every run, so a schema change needs no ALTER:
# a prior metadata.db with the older columns is simply replaced.
create_authors_table <- function(con) {
  DBI::dbExecute(con, "DROP TABLE IF EXISTS authors")
  DBI::dbExecute(con, "
  CREATE TABLE authors (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL,
    given   TEXT,
    family  TEXT,
    email   TEXT,
    role    TEXT,
    orcid   TEXT,
    ror_id  TEXT,
    comment TEXT
  )")
  DBI::dbExecute(con, "CREATE INDEX idx_authors_package ON authors (package)")
  DBI::dbExecute(con, "CREATE INDEX idx_authors_name    ON authors (family, given)")
  invisible(TRUE)
}

# Tables that carry state from run to run and cannot be rebuilt from CRAN.
STATE_TABLES <- c("check_status_history", "cran_check_deadlines", "cran_maintainer_bounces",
                  "cran_check_flavor_status_history", "cran_check_flavors")

# NULL when the prior release published no manifest; an error when it did but
# the file cannot be parsed.
read_prior_manifest <- function(path) {
  if (!file.exists(path)) return(NULL)
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

# The row counts a prior manifest promises for state tables: its state_tables
# key, or, for a manifest written before that key existed, the state tables
# among its tables.
prior_state_listing <- function(manifest) {
  if (is.null(manifest)) return(setNames(integer(0), character(0)))
  listed <- manifest$state_tables
  if (is.null(listed)) {
    listed <- manifest$tables
    listed <- listed[intersect(names(listed), STATE_TABLES)]
  }
  if (length(listed) == 0) return(setNames(integer(0), character(0)))
  setNames(as.integer(unlist(listed)), names(listed))
}

# Opens the downloaded metadata.db read-only and reads every state table in
# full, so a NUL byte or a damaged page shows up here and not halfway through
# update.R. ok is FALSE on any error, a listed table missing, or fewer rows
# than the prior manifest listed.
validate_prior_db <- function(path, manifest = NULL) {
  problems <- character(0)
  counts <- setNames(integer(0), character(0))
  listed <- prior_state_listing(manifest)
  con <- tryCatch(DBI::dbConnect(RSQLite::SQLite(), path, flags = RSQLite::SQLITE_RO,
                                 synchronous = NULL),
                  error = function(e) { problems <<- c(problems, conditionMessage(e)); NULL })
  if (!is.null(con)) {
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    tryCatch({
      qc <- DBI::dbGetQuery(con, "PRAGMA quick_check")[[1]]
      if (!identical(qc, "ok")) problems <- c(problems, paste("quick_check:", paste(qc, collapse = "; ")))
      present <- DBI::dbListTables(con)
      for (t in union(intersect(STATE_TABLES, present), names(listed))) {
        if (!t %in% present) {
          problems <- c(problems, sprintf("%s is listed with %d rows but missing", t, listed[[t]]))
          next
        }
        n <- nrow(DBI::dbGetQuery(con, sprintf('SELECT * FROM "%s"', t)))
        counts[[t]] <- n
        if (t %in% names(listed) && n < listed[[t]]) {
          problems <- c(problems, sprintf("%s has %d rows, the prior manifest listed %d", t, n, listed[[t]]))
        }
      }
    }, error = function(e) problems <<- c(problems, conditionMessage(e)))
  }
  list(ok = length(problems) == 0, problems = problems, counts = counts)
}

# What the validation step does with the downloaded db. start_fresh discards
# only a db that failed validation; a readable one is always kept.
prior_db_action <- function(valid, start_fresh = FALSE) {
  if (isTRUE(valid)) return("keep")
  if (isTRUE(start_fresh)) return("discard")
  "fail"
}

# How update.R starts. No db on disk is a cold start; when a prior release
# exists that is only allowed after the validation step discarded it.
startup_state <- function(db_exists, prior_release_exists, declared_cold) {
  if (isTRUE(db_exists)) return(list(cold_start = FALSE, error = NULL))
  if (isTRUE(prior_release_exists) && !isTRUE(declared_cold)) {
    return(list(cold_start = TRUE,
                error = "a prior release exists but no metadata.db was downloaded"))
  }
  list(cold_start = TRUE, error = NULL)
}

# The state tables among a manifest's table counts, as a JSON object.
state_table_counts <- function(tables) {
  keep <- intersect(STATE_TABLES, names(tables))
  setNames(lapply(keep, function(t) as.integer(tables[[t]])), keep)
}

# One line per state table with fewer rows than the prior manifest listed.
state_tables_shrunk <- function(current, prior_manifest) {
  listed <- prior_state_listing(prior_manifest)
  out <- character(0)
  for (t in names(listed)) {
    n <- if (t %in% names(current)) as.integer(current[[t]]) else 0L
    if (n < listed[[t]]) {
      out <- c(out, sprintf("%s has %d rows, the prior manifest listed %d", t, n, listed[[t]]))
    }
  }
  out
}
