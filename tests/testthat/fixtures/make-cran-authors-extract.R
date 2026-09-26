# Rebuilds cran-authors-extract.json from a saved tools::CRAN_authors_db() snapshot.
# Usage: Rscript make-cran-authors-extract.R <authors.rds> <out.json>
args <- commandArgs(trailingOnly = TRUE)
a <- readRDS(args[1])
orcid_re  <- "[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]"
review_re <- "github\\.com/(ropensci/(software-review|onboarding)|openjournals/joss-reviews)/issues/[0-9]+"
cm <- a$comment
m <- regexpr(review_re, gsub("[[:space:]]+", " ", cm), perl = TRUE)
link_end <- ifelse(m > 0, m + attr(m, "match.length") - 1L, NA)
picks <- c(
  which(is.na(a$ORCID) & grepl(orcid_re, cm)),
  which(is.na(a$ROR) & grepl("ror\\.org/", cm)),
  which(!is.na(link_end) & link_end > 120),
  which(!is.na(a$ORCID) & grepl(orcid_re, cm)),
  which(grepl("^https://ror\\.org/", a$ROR))[1:2],
  which(grepl("\t", cm, fixed = TRUE)),
  which(grepl("[[:alnum:]._%+-]+@[[:alnum:].-]+[.][a-z]{2,}", cm))[1:3],
  which(Encoding(cm) == "UTF-8")[1:3]
)
set.seed(20260925)
taken <- unique(picks)
taken <- c(taken, sample(setdiff(which(grepl("\n", cm, fixed = TRUE)), taken), 10))
taken <- c(taken, sample(setdiff(which(!is.na(cm)), taken), 40))
taken <- c(taken, sample(setdiff(which(is.na(cm)), taken), 200 - length(taken)))
out <- a[sort(taken), c("given", "family", "email", "role", "comment", "ORCID", "ROR", "package")]
rownames(out) <- NULL
# Real addresses stay out of git history; comments keep an address's shape.
out$email   <- NA_character_
out$comment <- gsub("[[:alnum:]._%+-]+@[[:alnum:].-]+[.][[:alpha:]]{2,}",
                    "someone@example.org", out$comment)
jsonlite::write_json(out, args[2], dataframe = "rows", na = "null", pretty = TRUE)
