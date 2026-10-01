# CRAN Metadata

Daily snapshots of CRAN package metadata: check results, check details, check issues, check status history, check deadlines, maintainer bounces, authors, package enrichment (URLs, bug trackers), archival reasons, and package NEWS. All data is stored in a single SQLite database (`metadata.db`) and published as a GitHub release.

## Data Access

### CLI

```bash
gh release download latest --repo r-observatory/cran-metadata --pattern "metadata.db"
```

### R

```r
url <- "https://github.com/r-observatory/cran-metadata/releases/latest/download/metadata.db"
download.file(url, "metadata.db", mode = "wb")

library(RSQLite)
con <- dbConnect(SQLite(), "metadata.db")

# Check results for a specific package
dbGetQuery(con, "SELECT * FROM cran_check_results WHERE package = 'ggplot2'")

# Check history over time
dbGetQuery(con, "
  SELECT package, status, detected_at
  FROM check_status_history
  WHERE package = 'dplyr'
  ORDER BY detected_at
")

# Authors of a package
dbGetQuery(con, "SELECT * FROM authors WHERE package = 'data.table'")

dbDisconnect(con)
```

### Python

```python
import urllib.request
import sqlite3

url = "https://github.com/r-observatory/cran-metadata/releases/latest/download/metadata.db"
urllib.request.urlretrieve(url, "metadata.db")

con = sqlite3.connect("metadata.db")
cur = con.cursor()

# Check results for a package
cur.execute("SELECT * FROM cran_check_results WHERE package = 'ggplot2'")
print(cur.fetchall())

# Status history
cur.execute("SELECT * FROM check_status_history WHERE package = 'dplyr' ORDER BY detected_at")
print(cur.fetchall())

con.close()
```

## Example Queries

### Packages with errors

```sql
SELECT DISTINCT package FROM cran_check_results WHERE status = 'ERROR';
```

### Check status history over time

```sql
SELECT package, status, flavor_summary, detected_at
FROM check_status_history
WHERE package = 'Rcpp'
ORDER BY detected_at;
```

### Find all authors with an ORCID

```sql
SELECT package, given, family, orcid
FROM authors
WHERE orcid IS NOT NULL AND orcid != '';
```

### Packages archived with a reason

```sql
SELECT package, reason FROM removal_reasons;
```

### Latest NEWS for a package

```sql
SELECT package, version, news_text
FROM package_news
WHERE package = 'jsonlite';
```

## Schema

### `cran_check_results`

Rebuilt each run. CRAN check results per package and flavor.

| Column | Type | Description |
|---|---|---|
| `package` | TEXT | Package name (PK part 1) |
| `flavor` | TEXT | R build flavor (PK part 2) |
| `status` | TEXT | OK, NOTE, WARNING, or ERROR |
| `tinstall` | REAL | Install time (seconds) |
| `tcheck` | REAL | Check time (seconds) |
| `ttotal` | REAL | Total time (seconds) |
| `version` | TEXT | Package version CRAN checked on this flavor. It can differ from the current CRAN version while a flavor catches up |
| `flags` | TEXT | Options the check ran with, such as `--no-vignettes` or `--no-tests`. NULL when it ran with none |

### `cran_check_details`

Rebuilt each run. Detailed check output per package and flavor.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Primary key (autoincrement) |
| `package` | TEXT | Package name |
| `flavor` | TEXT | R build flavor |
| `check_name` | TEXT | Name of the check |
| `status` | TEXT | Check status |
| `output` | TEXT | Check output text |

### `cran_check_issues`

Rebuilt each run. Known check issues per package.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Primary key (autoincrement) |
| `package` | TEXT | Package name |
| `version` | TEXT | Package version |
| `kind` | TEXT | Issue kind |
| `href` | TEXT | Link to issue details |

### `check_status_history`

**Append-only** -- accumulates over time, never dropped. Tracks when a package's worst check status changes.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Primary key (autoincrement) |
| `package` | TEXT | Package name |
| `status` | TEXT | Worst status across all flavors |
| `flavor_summary` | TEXT | JSON object with status counts, e.g. `{"OK":12,"NOTE":1}` |
| `details` | TEXT | JSON array of non-OK entries with flavor, status, check_name, output, version and flags (the last two are empty strings on rows written before they were recorded) |
| `detected_at` | TEXT | ISO 8601 timestamp when the change was detected |

### `cran_check_deadlines`

**Carried** from run to run. One row per episode of a CRAN "issues need fixing before" deadline (`tools::CRAN_package_db()$Deadline`).

| Column | Type | Description |
|---|---|---|
| `package` | TEXT | Package name (PK part 1) |
| `episode_seq` | INTEGER | 1 for the package's first deadline, +1 for each later one (PK part 2) |
| `deadline` | TEXT | The deadline as last seen; CRAN can move it |
| `version` | TEXT | CRAN version when the episode opened |
| `worst_status` | TEXT | Worst check status when the episode opened |
| `first_seen` | TEXT | First day this pipeline saw the deadline |
| `last_seen` | TEXT | Last day it was seen |
| `resolved_on` | TEXT | Day it was gone; NULL while open |
| `outcome` | TEXT | NULL while open, `met` when the package stayed on CRAN, `vanished` when it left |
| `archived_on` | TEXT | Filled downstream |
| `onset_known` | INTEGER | 0 when the episode was open on the run that created the table, so the deadline was set on or before `first_seen`; 1 when `first_seen` is the first day it was set; NULL for episodes opened before this column existed |

### `cran_maintainer_bounces`

**Carried** from run to run. One row per episode of CRAN's `Bounce` flag in `packages.rds`, which says CRAN's email to the package maintainer is undeliverable. The address itself is not stored.

| Column | Type | Description |
|---|---|---|
| `package` | TEXT | Package name (PK part 1) |
| `episode_seq` | INTEGER | 1 for the first episode, +1 each time the flag returns (PK part 2) |
| `version` | TEXT | CRAN version when the episode opened |
| `onset_known` | INTEGER | 0 when the flag was already set on the run that created the table (the onset is on or before `first_seen`), 1 otherwise |
| `first_seen` | TEXT | First day this pipeline saw the flag |
| `last_seen` | TEXT | Last day it was seen |
| `resolved_on` | TEXT | Day it was gone; NULL while open |
| `outcome` | TEXT | NULL while open, `cleared` when the package stayed on CRAN, `vanished` when it left |
| `archived_on` | TEXT | Filled downstream |

### `authors`

Rebuilt each run. Author information from CRAN (`tools::CRAN_authors_db()`).

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Primary key (autoincrement) |
| `package` | TEXT | Package name |
| `given` | TEXT | Given name |
| `family` | TEXT | Family name |
| `email` | TEXT | Email address |
| `role` | TEXT | Role (aut, cre, ctb, etc.) |
| `orcid` | TEXT | ORCID identifier |
| `ror_id` | TEXT | ROR identifier |
| `comment` | TEXT | Free-text comment from the person entry, on one line and never truncated. NULL when CRAN recorded none, when it held nothing but an identifier moved out of it (see below), or on a run where the comment step failed |

When `orcid` is empty and the comment holds exactly one distinct ORCID iD that passes its check digit, that iD is written to `orcid`. When `ror_id` is empty and the comment holds exactly one distinct `ror.org/` id, that id is written to `ror_id`. An identifier written twice counts once. Every occurrence of a moved identifier is then removed together with what directly precedes it and one closing quote or `>` right after it. What precedes it is, in this order and each part optional: a label (`ORCID`, `ORCID iD`, `ROR` or `ROR ID`, in any letter case) followed by spaces and at most one `:` or `=`, or by nothing; one quote (`"` or `'`) or `<`; an `orcid.org/` or `ror.org/` prefix with or without `http://`, `https://` or `www.`. If only punctuation and spaces remain, the comment is stored as NULL. Otherwise it is kept whole, identifier included, so a label after the identifier (`<iD> (ORCID)`) or a bracket between the label and the identifier (`ORCID: (<iD>)`) keeps the comment.

If cleaning or normalizing the comments fails on a run, that run stores `comment` as NULL on every row, keeps `orcid` and `ror_id` as CRAN gave them (nothing is moved that day), writes every other column as usual and logs one `WARN:` line in the Authors section. The same section's `Comments kept: N | ORCID iDs moved from comments: N | ROR ids moved from comments: N` line gives each run's counts.

### `packages_enrichment`

Rebuilt each run. Package URLs and bug report links.

| Column | Type | Description |
|---|---|---|
| `name` | TEXT | Package name (PK) |
| `url` | TEXT | Package URL(s) |
| `bug_reports` | TEXT | Bug report URL |

### `removal_reasons`

Rebuilt each run. Archival reasons scraped from CRAN for packages with ERROR status.

| Column | Type | Description |
|---|---|---|
| `package` | TEXT | Package name (PK) |
| `reason` | TEXT | Archival reason from CRAN page |

### `package_news`

Rebuilt each run. NEWS content from the GitHub CRAN mirror.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Primary key (autoincrement) |
| `package` | TEXT | Package name |
| `version` | TEXT | Package version |
| `news_text` | TEXT | First version section of NEWS (up to 2000 chars) |

## Update Schedule

The database is updated daily at 06:00 UTC via GitHub Actions. Each run rebuilds the snapshot tables from scratch, appends new entries to `check_status_history` when a package's worst check status changes, and updates the episode tables. The latest database is always available from the most recent GitHub release.

## Carried state

`check_status_history`, `cran_check_deadlines` and `cran_maintainer_bounces` cannot be rebuilt from CRAN, so each run starts from the previous release's `metadata.db`. The run downloads that database and its `manifest.json` from one release tag and stops before touching anything when GitHub fails to list or serve either file, when the database cannot be opened and read, or when a table the manifest lists under `state_tables` is missing or has fewer rows than listed. It also refuses to publish a database whose state tables have fewer rows than the previous manifest listed.

The one way past an unreadable database is a manual run with the `start_fresh` input. It discards the database only when it fails those checks. That run's release notes then carry a "Cold start" line, its manifest has `cold_start: true`, and every episode it opens has `onset_known = 0`.

## Retention

No dated release of this repository, and no asset of one, is deleted, except a draft or a deletion the owner approves for named tags. The owner approves one only when nothing is left that exists only in those tags: each table below is either extracted through them into the history asset (`r-observatory/data`, tag `history`, per its manifest) or marked abandoned by the owner.

- Extracted into the history asset: per-flavor check status, check timings, check issues and deadline moves.
- Carried whole by every release: `check_status_history`, `cran_check_deadlines` and `cran_maintainer_bounces`.
- Only in the dated releases: `cran_check_details` (the day's non-OK check output), `authors` and `packages_enrichment` as CRAN edits them between package versions, `package_news`, and `removal_reasons`.

## License

The data is sourced from [CRAN](https://cran.r-project.org/), which is maintained by the R Foundation. This repository provides the pipeline infrastructure and daily snapshots. Please respect CRAN's terms of use.

## Feedback

Found a bug, a wrong number, or a missing package? Report it at [r-observatory/feedback](https://github.com/r-observatory/feedback/issues/new/choose). All feedback about R Observatory, the site, the data, and the pipelines, is tracked in one place.
