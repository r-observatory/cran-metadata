# CRAN Metadata

Daily snapshots of CRAN package metadata: check results, check details, check issues, check status history, authors, package enrichment (URLs, bug trackers), archival reasons, and package NEWS. All data is stored in a single SQLite database (`metadata.db`) and published as a GitHub release.

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
| `details` | TEXT | JSON array of non-OK entries with flavor, status, check_name, output |
| `detected_at` | TEXT | ISO 8601 timestamp when the change was detected |

### `authors`

Rebuilt each run. Author information from CRAN.

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

The database is updated daily at 06:00 UTC via GitHub Actions. Each run rebuilds all live tables from scratch and appends new entries to `check_status_history` when a package's worst check status changes. The latest database is always available from the most recent GitHub release.

## License

The data is sourced from [CRAN](https://cran.r-project.org/), which is maintained by the R Foundation. This repository provides the pipeline infrastructure and daily snapshots. Please respect CRAN's terms of use.
