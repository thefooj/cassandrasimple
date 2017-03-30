

as_date_from_cql_date <- function(localdate) {
  as.Date(rJava::.jcall(localdate, 'S', 'toString'), tz="UTC") # to YYYY-MM-DD
}
as_posixct_from_cql_get_timestamp <- function(javaDate) {
  # getTime gives milliseconds since epoch 1970-01-01 as a Java long (64-bit signed).
  # we convert to seconds and give the epoch
  as.POSIXct(rJava::.jcall(javaDate,'J', 'getTime')/1000, origin="1970-01-01", tz="UTC")
}



as_date_from_1970_epoch_days <- function(data) {
  as.Date(data, origin="1970-01-01", tz="UTC")
}

as_posixct_from_1970_epoch_seconds <- function(data) {
  as.POSIXct(data, origin="1970-01-01", tz="UTC")
}