# Loaded as R_PROFILE_USER when tests run scripts/update.R: CRAN's readers
# return the frames saved at CRAN_STUB_FIXTURE, and web reads fail at once.
local({
  fx <- readRDS(Sys.getenv("CRAN_STUB_FIXTURE"))
  readers <- c(CRAN_check_results = "check_results", CRAN_check_details = "check_details",
               CRAN_check_issues = "check_issues", CRAN_authors_db = "authors",
               CRAN_package_db = "package_db")
  ns <- asNamespace("tools")
  for (fn in names(readers)) {
    stub <- local({
      v <- fx[[readers[[fn]]]]
      function(...) if (is.null(v)) stop("stubbed CRAN read failed") else v
    })
    unlockBinding(fn, ns)
    assign(fn, stub, envir = ns)
    lockBinding(fn, ns)
  }
})
readLines <- function(con = stdin(), ...) {
  if (is.character(con) && grepl("^https?://", con)) stop("no network in tests")
  base::readLines(con, ...)
}
