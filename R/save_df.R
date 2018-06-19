
.coerce_df_to_chars <- function(df) {
  dfCopy <- df
  for (x in colnames(df)) {
    #cat("Coercing to char: ",x,"\n")

    colClass = class(df[,x])
    if (length(colClass) > 1) {
      colClass = colClass[1]
    }

    # special case of all na
    if (all(is.na(dfCopy[,x]))) {
      dfCopy[,x] <- 'NULL'
    } else if (colClass=='numeric') {
      dfCopy[,x] <- as.character(round(df[,x],5))  # use 5 for now so we don't over-push details.  By rounding, any integer values will show as integers (3.000 -> 3)
    } else if(colClass=='character' || colClass=='factor') {
      dfCopy[,x] <- paste("'",gsub("'","''",as.character(df[,x]),fixed=T),"'",sep='')
    } else if (colClass=='Date') {
      dfCopy[,x] <- paste("'",as.character(df[,x]),"'",sep='')
    } else if (colClass=='POSIXct') {
      dfCopy[,x] <- format(df[,x], "'%Y-%m-%dT%H:%M:%S+0000'",tz='UTC')
    } else if (colClass=='integer') {
      dfCopy[,x] <- as.character(df[,x])
    } else if (colClass=='logical') {  # boolean T/F/NA
      dfCopy[,x] <- sapply(df[,x], function(x) { ifelse(is.na(x), NA,  ifelse(x,'true','false') ) })
    } else {
      stop("unknown df class for ", x, " (",colClass,")")
    }
    dfCopy[which(is.na(df[,x])),x] <- 'NULL'
  }
  return(dfCopy)
}

# returns vector of CQL of strings that are CQL INSERT statements
.df_to_cql <- function(df, tbl) {
  df_chars <- .coerce_df_to_chars(df=df)

  cols_csv <- paste(colnames(df),collapse=',',sep='')
  insert_head <- paste0("INSERT INTO ",tbl," (",cols_csv, ") VALUES")

  paste(insert_head, " (",apply(df_chars,1,function(row) {
    paste(row,collapse=',',sep='')
  }), ");\n",sep='')
}


#' Save a dataframe to Cassandra for the given table name
#' We currently handle:
#'   numeric -> float (autoconvert to int).  Floats are rounded to 5 digits currently
#'   date -> 'YYYY-mm-dd' strings
#'   POSIXct -> 'YYYY-mm-ddTHH:MM:SS+0000' in UTC.
#'   logical -> 1, 0, or NA.  If a column is all-NA, then the data.frame thinks it's 'logical'.  We save it as NA.
#'   character -> strings enclosed in single quotes, and escaped the CQL way.  e.g. 'It''s my party'
#'   all-NA  -> NA
#' NAs are represented as NULL in Cassandra, so we handle that correctly
#' The user of this function must specify a reasonable row_batches to prevent write failures.
#' If the batch is too large, you will bump against the batch_size_fail_threshold_in_kb limit in Cassandra (default 50kb)
#' If the batch is too small, the writes will take too long
#'
#' @param jCassSess The Cassandra session, from get_cass_session
#' @param df Data.frame.  Columns must match columns in the Cassandra table.  Otherwise the save will fail
#' @param table_name Name of the Cassandra table.  Save will fail if table does not exist
#' @param row_batches Number of inserts to run in each batch.  Batches are wrapped in BEGIN BATCH ... APPLY BATCH.  This implies logging.
#' @param sleep_between_batches Number of seconds to sleep between batches.  Defaults to 0.
cass_save_df <- function(jCassSess, df, table_name, row_batches = 1000, sleep_between_batches = 0, show_debug=F) {

  # do not use this function when you are working with a df that contains over 100,000 rows

  if (missing(table_name) | is.null(table_name) | identical(table_name, NA)) {
    stop('Invalid table name!')
  }

  if (!grepl('^[0-9a-z\\_]+$', table_name, perl = TRUE)) {
    stop('Illegal characters in the table name!')
  }

  if (sum(grepl('^[0-9a-z\\_]+$', names(df), perl = TRUE)) != ncol(df)) {
    stop('Illegal characters in the df column names!')
  }

  if(is.null(row_batches) | identical(row_batches, NA) | !identical(class(row_batches), 'numeric')) {
    stop('Invalid row batches!')
  }

  if (nrow(df) == 0) {
    cat("cass_save_df called with empty data frame.  Doing nothing")
    return(T)
  }


  batch_prefix <- 'BEGIN BATCH '
  batch_suffix <- ' APPLY BATCH'

  #each time, we only upload up to 1000 (row_batches) rows of data with one CQL insert command.
  #separate data frame into small group with no more than 1000 rows of data
  num_uploads <- ceiling(nrow(df) / row_batches)
  options(digits.secs=6)

  tot_prep_time <- 0
  tot_upload_time <- 0
  tot_sleep_time <- 0

  for (i in 1:num_uploads) {
    prep_start_time <- Sys.time()

    if (i*row_batches <= nrow(df)) {
      df_subset <- dplyr::slice(df, ((i - 1)*row_batches + 1):(i*row_batches))
    } else {
      df_subset <- dplyr::slice(df, ((i - 1)*row_batches + 1):nrow(df))
    }

    insert_command_line <- .df_to_cql(df_subset, table_name)

    combined_result <- paste0(batch_prefix, paste0(insert_command_line, collapse = ""), batch_suffix)

    prep_stop_time <- Sys.time()

    cass_update(jCassSess, cqlQuery = combined_result)

    upload_stop_time <- Sys.time()

    if (show_debug) {
      cat("[SaveDfToCassandra:",table_name," batch=",i," @", Sys.time(), " - prep: ", (prep_stop_time - prep_start_time), "   upload: ", (upload_stop_time - prep_stop_time),"\n")
    }

    tot_prep_time <- tot_prep_time + (prep_stop_time - prep_start_time)
    tot_upload_time <- tot_upload_time + (upload_stop_time - prep_stop_time)

    if (sleep_between_batches > 0) {
      if (show_debug) {
        cat("[SaveDfToCassandra:",table_name," sleep ", sleep_between_batches,"\n")
      }
      Sys.sleep(sleep_between_batches)
    }

    tot_sleep_time <- tot_sleep_time + sleep_between_batches
  }

  options(digits.secs=NULL)

  if (show_debug) {
    cat("Total prep: ",tot_prep_time,"\n")
    cat("Total upload: ", tot_upload_time,"\n")
    cat("Total sleep: ", tot_sleep_time,"\n")

    cat('We have uploaded', i, 'batches to Cassandra.\n')
  }

  return(T)
}
