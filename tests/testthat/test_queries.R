library(cassandrasimple)
library(dplyr)

test_that("updates and queries work, converting NULL to NA and handling mappings for our supported types", {


  jCassSess <- get_cass_session('localhost', 'Test Cluster', 'cassandrasimple_test')

  cass_update(jCassSess, "drop table if exists test_table")

  cass_update(jCassSess, "create table if not exists test_table (
              dt date,
              hr tinyint,
              obj_id int,
              float_1 float,
              float_2 float,
              double_1 double,
              long_1 varint,
              long_2 bigint,
              str_val text,
              primary key ((dt),hr,obj_id)
  )")

  res <- cass_query(jCassSess, "SELECT * FROM test_table")
  expect_is(res, "data.frame")
  expect_equal(nrow(res),0)

  cass_update(jCassSess, "insert into test_table (dt,hr,obj_id,float_1,float_2,double_1,long_1, long_2,str_val) values ('2017-01-19',13,80,24.33,-34.33,0.33314,9223372036854775807, 9223372036854775807,'HELLO THERE HOW ARE YOU')")
  cass_update(jCassSess, "BEGIN BATCH
              insert into test_table (dt,hr,obj_id,float_1,float_2,double_1,long_1,long_2,str_val) values ('2017-01-20',13,80,NULL,-34.33,NULL,9223372036854775807, 9223372036854775807,NULL);
              insert into test_table (dt,hr,obj_id,float_1,float_2,double_1,long_1,long_2,str_val) values ('2017-01-20',24,80,14.44,55.55,8.2492994,NULL, NULL,'ANOTHER');
              APPLY BATCH")

  #           dt    hr obj_id double_1 float_1 float_2 long_1         long_2            str_val
  #       (date) (int)  (int)    (dbl)   (dbl)   (dbl)   (num)        (num)              (chr)   <<<--- R types
  # 1 2017-01-20    13     80       NA      NA  -34.33 9.223372e+18 9.223372e+18             NA
  # 2 2017-01-20    24     80 8.249299   14.44   55.55           NA           NA         ANOTHER
  # 3 2017-01-19    13     80 0.333140   24.33  -34.33 9.223372e+18 9.223372e+18 HELLO THERE HOW ARE YOU

  res <- cass_query(jCassSess, "SELECT * FROM test_table")

  expect_is(res, "data.frame")
  expect_equal(nrow(res),3)

  expect_equal(res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id ==80) %>% select(str_val) %>% as.character(), 'ANOTHER')
  expect_true(abs(res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id ==80) %>% select(float_1) %>% as.numeric() - 14.44) < 0.0001)
  expect_true(abs(res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id ==80) %>% select(float_2) %>% as.numeric() - -34.33) < 0.0001)
  expect_true(abs(res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id ==80) %>% select(long_1) %>% as.numeric() - 9223372036854775807) < 0.0001)
  expect_true(abs(res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id ==80) %>% select(long_2) %>% as.numeric() - 9223372036854775807) < 0.0001)
  # int handling
  expect_equal(as.character(format(res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id == 80) %>% select(hr) )), '24')

  # date handling
  expect_equal(as.character(format(res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id == 80) %>% select(dt) )), '2017-01-20')

  # verify NULL cases
  expect_true(is.na((res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id == 80) %>% select(str_val))$str_val))
  expect_true(is.na((res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id == 80) %>% select(float_1))$float_1))
  expect_true(is.na((res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id == 80) %>% select(double_1))$double_1))
  expect_true(is.na((res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id == 80) %>% select(long_1))$long_1))
  expect_true(is.na((res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id == 80) %>% select(long_2))$long_2))
  close_cass_session(jCassSess)
})

test_that("timestamp types are retrieved as UTC with appropriate conversions", {
  jCassSess <- get_cass_session('localhost', 'Test Cluster', 'cassandrasimple_test')

  cass_update(jCassSess, "drop table if exists test_timestamp_table")

  cass_update(jCassSess, "create table if not exists test_timestamp_table (
              dt date,
              hr tinyint,
              obj_id int,
              update_version_time timestamp,
              float_1 float,
              float_2 float,
              double_1 double,
              str_val text,
              primary key ((dt),hr,obj_id, update_version_time)
  )")

  # inserts based on discernable strings for timezone offsets
  cass_update(jCassSess, "BEGIN BATCH
              insert into test_timestamp_table (dt,hr,obj_id,update_version_time,float_1,float_2,double_1,str_val) values ('2017-01-20',13,80,'2017-01-19 07:05:02+0000',NULL,-34.33,NULL,NULL);
              insert into test_timestamp_table (dt,hr,obj_id,update_version_time,float_1,float_2,double_1,str_val) values ('2017-01-20',24,80,'2017-01-19 02:04:02-0500',14.44,55.55,8.2492994,'ANOTHER');
              APPLY BATCH")

  res <- cass_query(jCassSess, "SELECT * FROM test_timestamp_table")
  expect_is(res, "data.frame")
  expect_equal(nrow(res),2)
  expect_is(res$update_version_time, 'POSIXct')
  # converted over to UTC
  expect_equal(strftime(res$update_version_time, '%Y-%m-%d %H:%M:%S %Z', tz="UTC"),
               c('2017-01-19 07:05:02 UTC','2017-01-19 07:04:02 UTC'))


  close_cass_session(jCassSess)
})