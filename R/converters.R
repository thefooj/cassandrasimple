

as_date_from_cql_date <- function(localdate) {
  as.Date(rJava::.jcall(localdate, 'S', 'toString')) # to YYYY-MM-DD
}