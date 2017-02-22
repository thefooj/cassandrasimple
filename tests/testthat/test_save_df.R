library(cassandrasimple)
library(dplyr)

test_that("cass_save_df: updates work with proper type conversions and NA handling", {
  jCassSess <- get_cass_session('localhost', 'Tios001', 'cassandrasimple_test')

  cass_update(jCassSess, "drop table if exists test_insert_table")

  cass_update(jCassSess, "create table if not exists test_insert_table (
              dt date,
              hr tinyint,
              obj_id int,
              int_1 int,
              int_2 int,
              float_1 float,
              float_2 float,
              double_1 double,
              str_val text,
              factor_str_val text,
              ts_val timestamp,
              dt_val date,
              primary key ((dt),hr,obj_id)
  )")

  set.seed(13247)

  # test simplest save
  myDf1 <- data.frame(dt=as.Date('2017-02-22'), hr=seq(1,24), obj_id=round(runif(24,min=1,max=1000),0),
                      int_1=as.integer(runif(24,min=1,max=24)),
                      int_2=as.numeric(seq(1,24)),
                      float_1=runif(24,min=-10,max=10),
                      float_2=runif(24,min=-1000,max=1000),
                      double_1=runif(24,min=-1000,max=1000),
                      str_val=paste("Hi! It's hour",seq(1,24)),
                      factor_str_val=as.factor(paste("fac_",round(runif(24,min=1,max=100),0),sep='')),
                      ts_val=as.POSIXct('2017-02-22 00:23:33',tz='EST'),
                      dt_val=as.Date('2017-03-01'),
                      stringsAsFactors = F
                      )
  myDf1[3,]$float_1 <- NA
  myDf1[3,]$str_val <- NA
  myDf1[3,]$factor_str_val <- NA
  myDf1[4,]$double_1 <- NA
  myDf1[4,]$dt_val <- NA
  myDf1[5,]$int_1 <- NA
  myDf1[5,]$ts_val <- NA

  cass_save_df(jCassSess, myDf1, "test_insert_table", row_batches=2)

  res_df <- cass_query(jCassSess, "select * from test_insert_table")
  res_df <- arrange(res_df, hr)

  # test NA cases
  expect_true(is.na(filter(res_df, dt=='2017-02-22', hr==3)$str_val))
  expect_true(is.na(filter(res_df, dt=='2017-02-22', hr==3)$float_1))
  expect_true(is.na(filter(res_df, dt=='2017-02-22', hr==3)$factor_str_val))
  expect_true(is.na(filter(res_df, dt=='2017-02-22', hr==4)$dt_val))
  expect_true(is.na(filter(res_df, dt=='2017-02-22', hr==4)$double_1))
  expect_true(is.na(filter(res_df, dt=='2017-02-22', hr==5)$int_1))
  expect_true(is.na(filter(res_df, dt=='2017-02-22', hr==5)$ts_val))

  # test float
  expect_true(abs(filter(res_df, dt=='2017-02-22', hr==5)$float_1 - (-8.41989)) <= 0.0001)
  expect_true(abs(filter(res_df, dt=='2017-02-22', hr==5)$float_2 - (-780.6906)) <= 0.0001)

  # test string escaping
  expect_equal(filter(res_df, dt=='2017-02-22', hr==5)$str_val,"Hi! It's hour 5")

  # test coersion of ints in numeric form to ints
  expect_equal(res_df$hr,seq(1,24))
  expect_equal(res_df$int_2,seq(1,24))

  # test date conversion
  expect_equal(filter(res_df, dt=='2017-02-22', hr==1)$dt_val, as.Date('2017-03-01'))

  # test POSIXct converstion
  expect_equal(strftime(filter(res_df, dt=='2017-02-22', hr==1)$ts_val, format='%Y-%m-%d %H:%M:%S %z',tz='UTC'),'2017-02-22 05:23:33 +0000')

  # factor to string
  expect_equal(class(res_df$factor_str_val),'character')


  # test 1 row case
  myDf1row <- myDf1[1,]
  myDf1row$dt = as.Date('2017-02-23')
  myDf1row$float_1 <- NA
  myDf1row$int_1 <- NA
  myDf1row$str_val <- NA

  cass_save_df(jCassSess, myDf1row, "test_insert_table", row_batches=2)

  res_1row_df <- cass_query(jCassSess, "select * from test_insert_table where dt = '2017-02-23'")
  expect_equal(nrow(res_1row_df),1)
  expect_equal(res_1row_df$dt, as.Date('2017-02-23'))
  expect_equal(res_1row_df$hr, as.integer(1))
  expect_equal(strftime(filter(res_1row_df, dt=='2017-02-23', hr==1)$ts_val, format='%Y-%m-%d %H:%M:%S %z',tz='UTC'),'2017-02-22 05:23:33 +0000')

  expect_true(is.na(filter(res_1row_df, dt=='2017-02-23', hr==1)$float_1))
  expect_true(is.na(filter(res_1row_df, dt=='2017-02-23', hr==1)$int_1))
  expect_true(is.na(filter(res_1row_df, dt=='2017-02-23', hr==1)$str_val))

  expect_equal(class(res_1row_df$factor_str_val),'character')
  expect_equal(res_1row_df$factor_str_val,'fac_18')
  # test 0 rows case

  # do nothing for empty df
  output_message <- capture.output(cass_save_df(jCassSess, myDf1row[0,], "test_insert_table", row_batches=2))
  expect_match(output_message[1], "cass_save_df called with empty data frame.  Doing nothing")



  cass_update(jCassSess, "drop table if exists test_insert_table")

  close_cass_session(jCassSess)
})


test_that("cass_save_df: defensive code cases are handled for bad inputs", {


  jCassSess <- get_cass_session('localhost', 'Tios001', 'cassandrasimple_test')

  cass_update(jCassSess, "drop table if exists test_simple_table")

  cass_update(jCassSess, "create table if not exists test_simple_table (
              dt date,
              hr tinyint,
              int_1 int,
              primary key ((dt),hr)
  )")

  set.seed(13247)
  myDf1 <- data.frame(dt=as.Date('2017-02-22'), hr=seq(1,24),
                      int_1=as.integer(runif(24,min=1,max=24)),
                      stringsAsFactors = F)

  expect_error(cass_save_df(jCassSess, myDf1, NA, 1000), 'Invalid table name')
  expect_error(cass_save_df(jCassSess, myDf1, 'bad table name', 1000), 'Illegal characters in the table name')

  expect_error(cass_save_df(jCassSess, myDf1, 'ok_table', NA), 'Invalid row batches')
  expect_error(cass_save_df(jCassSess, myDf1, 'ok_table', NULL), 'Invalid row batches')
  expect_error(cass_save_df(jCassSess, myDf1, 'ok_table', "hello"), 'Invalid row batches')


  myDfBadCol1 <- data.frame(dt=as.Date('2017-02-22'), hr=seq(1,24),
                      "another.bad.col"=as.integer(runif(24,min=1,max=24)),
                      stringsAsFactors = F)
  expect_error(cass_save_df(jCassSess, myDfBadCol1, 'test_simple_table', 1000), 'Illegal characters in the df column names')

  myDfBadCol2 <- data.frame(dt=as.Date('2017-02-22'), hr=seq(1,24),
                           "another.bad.col"=as.integer(runif(24,min=1,max=24)),
                           stringsAsFactors = F)
  expect_error(cass_save_df(jCassSess, myDfBadCol2, 'test_simple_table', 1000), 'Illegal characters in the df column names')




  close_cass_session(jCassSess)

})