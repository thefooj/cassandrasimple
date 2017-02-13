
#' for a given Java com.datastax.driver.core.ResultSet object, get the column definitions as a List.
#' Raises errors if we do not yet support the column type.
#' Currently we support: float, double, text, varchar, int, tinyint, date
#'
#' @param jcRes instance of com.datastax.driver.core.ResultSet
.cass_col_map <- function(jcRes) {

  jcColDefs <- rJava::.jcall(jcRes, 'Lcom/datastax/driver/core/ColumnDefinitions;', 'getColumnDefinitions')

  numCols <- rJava::.jcall(jcColDefs, 'I', 'size')

  mappings = list()
  for (i in 1:numCols) {
    cname <- rJava::.jcall(jcColDefs, "S", "getName", as.integer(i-1))
    ctypeObj <- rJava::.jcall(jcColDefs, 'Lcom/datastax/driver/core/DataType;','getType', as.integer(i-1))
    ctype <- rJava::.jcall(ctypeObj, 'S', 'asFunctionParameterString')

    map = list()
    map$index = as.integer(i-1)
    map$javatype = ctype
    map$colname = cname

    if (ctype=='float') {
      map$java_get_func = 'getFloat'
      map$r_cast_func   = 'as.numeric'
      map$jni_type      = 'F'
    } else if (ctype=='double') {
      map$java_get_func = 'getDouble'
      map$r_cast_func   = 'as.numeric'
      map$jni_type      = 'D'
    } else if (ctype=='text'||ctype=='varchar') {
      map$java_get_func = 'getString'
      map$r_cast_func   = 'as.character'
      map$jni_type      = 'S'
    } else if (ctype=='int') {
      map$java_get_func = 'getInt'
      map$r_cast_func   = 'as.integer'
      map$jni_type      = 'I'
    } else if (ctype=='tinyint') {
      map$java_get_func = 'getByte'
      map$r_cast_func   = 'as.integer'
      map$jni_type      = 'B'
    } else if (ctype=='date') {
      map$java_get_func = 'getDate'
      map$r_cast_func   = 'as_date_from_cql_date'  # our custom function
      map$jni_type      = 'Lcom/datastax/driver/core/LocalDate;'
    } else {
      stop(paste0("Unsupported type of column ",cname," type ",ctype," in query "))
    }
    mappings[[cname]] <- map
  }
  return(mappings)
}



#' Perform a CQL update.
#'
#' @param jCassSess The Cassandra session, from get_cass_session
#' @param cqlQuery A string query.  If performing multiple at once, use BEG
#'
#' @examples
#'   cass_update(jCassSess, "insert into test_lmp_combined (dt,hr,official_pnode_id,rt_energy,da_energy) values ('2017-01-19',13,80,24.33,-34.33)")
#'   cass_update(jCassSess, "BEGIN BATCH
#'       insert into test_lmp_combined (dt,hr,official_pnode_id,rt_energy,da_energy) values ('2017-01-20',13,80,24.33,-34.33);
#'       insert into test_lmp_combined (dt,hr,official_pnode_id,rt_energy,da_energy) values ('2017-01-20',24,80,14.44,55.55);
#'       APPLY BATCH")
#' @export
cass_update <- function(jCassSess, cqlQuery) {
  rJava::.jcall(jCassSess, 'Lcom/datastax/driver/core/ResultSet;', 'execute', cqlQuery)
  return(T)
}

#' Execute a CQL query and capture the results as a data.frame.  Columns will match up with what is in the Cassandra table definition and your query.
#' Follow CQL rules for queries.  If you try to query on a column without a secondary index, you will get errors from the Datastax driver.
#' NULL values in the Cassandra tables are returned as NA in the data frame.  We convert Cassandra data types to reasonable R equivalents.
#' We currently support the following mappings:
#'   float, double, text, varchar, int, tinyint, date
#' See .cass_col_map for mapping details.
#'
#' @example stuff <- cass_query(jCassSess,"select * from test_lmp_combined where dt = '2017-01-20' and hr = 13")
#' @param jCassSess The Cassandra session, from get_cass_session
#' @param cqlQuery A string query.
#' @export
cass_query <- function(jCassSess, cqlQuery) {
  jcRes <- rJava::.jcall(jCassSess, 'Lcom/datastax/driver/core/ResultSet;', 'execute', cqlQuery)

  results = list()

  colMaps <- .cass_col_map(jcRes)

  ctr <- 0
  while(!rJava::.jcall(jcRes, 'Z', 'isExhausted')) {
    jcRow <- rJava::.jcall(jcRes, 'Lcom/datastax/driver/core/Row;', 'one')
    ctr <- ctr+1
    row = list()
    for (m in names(colMaps)) {
      if (rJava::.jcall(jcRow, 'Z', "isNull", m)) {
        val <- NA
      } else {
        val <- rJava::.jcall(jcRow, colMaps[[m]]$jni_type, colMaps[[m]]$java_get_func, m)
        val <- get(colMaps[[m]]$r_cast_func)(val)
      }
      row[[m]] <- val
    }
    results[[ctr]] <- row
  }
  return(dplyr::bind_rows(results))
}