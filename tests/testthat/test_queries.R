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
              str_val text,
              primary key ((dt),hr,obj_id)
  )")

  res <- cass_query(jCassSess, "SELECT * FROM test_table")
  expect_is(res, "data.frame")
  expect_equal(nrow(res),0)

  cass_update(jCassSess, "insert into test_table (dt,hr,obj_id,float_1,float_2,double_1,str_val) values ('2017-01-19',13,80,24.33,-34.33,0.33314,'HELLO THERE HOW ARE YOU')")
  cass_update(jCassSess, "BEGIN BATCH
              insert into test_table (dt,hr,obj_id,float_1,float_2,double_1,str_val) values ('2017-01-20',13,80,NULL,-34.33,NULL,NULL);
              insert into test_table (dt,hr,obj_id,float_1,float_2,double_1,str_val) values ('2017-01-20',24,80,14.44,55.55,8.2492994,'ANOTHER');
              APPLY BATCH")

  #           dt    hr obj_id double_1 float_1 float_2                 str_val
  #       (date) (int)  (int)    (dbl)   (dbl)   (dbl)                   (chr)
  # 1 2017-01-20    13     80       NA      NA  -34.33                      NA
  # 2 2017-01-20    24     80 8.249299   14.44   55.55                 ANOTHER
  # 3 2017-01-19    13     80 0.333140   24.33  -34.33 HELLO THERE HOW ARE YOU

  res <- cass_query(jCassSess, "SELECT * FROM test_table")
  expect_is(res, "data.frame")
  expect_equal(nrow(res),3)

  expect_equal(res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id ==80) %>% select(str_val) %>% as.character(), 'ANOTHER')
  expect_true(abs(res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id ==80) %>% select(float_1) %>% as.numeric() - 14.44) < 0.0001)
  expect_true(abs(res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id ==80) %>% select(float_2) %>% as.numeric() - -34.33) < 0.0001)

  # int handling
  expect_equal(as.character(format(res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id ==80) %>% select(hr) )), '24')

  # date handling
  expect_equal(as.character(format(res %>% filter(dt == '2017-01-20' & hr == 24 & obj_id ==80) %>% select(dt) )), '2017-01-20')

  # verify NULL cases
  expect_true(is.na((res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id ==80) %>% select(str_val))$str_val))
  expect_true(is.na((res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id ==80) %>% select(float_1))$float_1))
  expect_true(is.na((res %>% filter(dt == '2017-01-20' & hr == 13 & obj_id ==80) %>% select(double_1))$double_1))

  close_cass_session(jCassSess)
})