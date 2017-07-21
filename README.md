# cassandrasimple
R package to connect to Cassandra and do simple operations

This package wraps the Datastax Cassandra Java Driver version 3.1 in R.

We use rJava to interface with Java.  Using the Jar files provided by Datastax, we initiate Cassandra sessions, and the run CQL queries.

For reading data, we inspect the resultset data types and construct data.frame with the appropriate column types and conversion of Cassandra NULL to R NA.

```
jCassSess <- get_cass_session('cassandracluster-internal.mycompany.com', 'myCluster', 'myKeyspace')

cass_update(jCassSess, "create table test_table (
    dt date,
    hr tinyint, 
    obj_id int, 
    val_1 float,
    val_2 float,
    primary key ((dt),hr,obj_id)  
)")

cass_update(jCassSess, "insert into test_table (dt,hr,obj_id,val_1,val_2) values ('2017-01-19',13,80,24.33,-34.33)")
cass_update(jCassSess, "BEGIN BATCH
      insert into test_table (dt,hr,obj_id,val_1,val_2) values ('2017-01-20',13,80,24.33,-34.33);
      insert into test_table (dt,hr,obj_id,val_1,val_2) values ('2017-01-20',24,80,14.44,55.55);
    APPLY BATCH")
    
stuff1 <- cass_query(jCassSess,"select * from test_table where dt = '2017-01-20' and hr = 13")    
# 
#           dt    hr obj_id val_1  val_2
# 1 2017-01-20    13     80 24.33 -34.33
# 2 2017-01-20    24     80 14.44  55.55

stuff2 <- cass_query(jCassSess,"select * from test_table where dt = '2017-01-20' and hr = 13")  
# 
#          dt    hr obj_id val_1  val_2
#1 2017-01-20    13     80 24.33 -34.33

close_cass_session(jCassSess)
```

To install this, do:

```
library(devtools)
install_github("thefooj/cassandrasimple")
```

We currently support these types:

* float => as.numeric
* double => as.numeric
* text => as.character
* varchar => as.character
* int => as.integer
* tinyint => as.integer
* date => as.Date.  Custom converter using CQL LocalDate to int days since 1970-01-01 and to asDate(x, origin='1970-01-01', tz='UTC')
* timestamp => as.POSIXct.  Custom converter using CQL Timestamp (milliseconds since epoch 1970-01-01) to long milliseconds -> seconds to asPOSIXct(x, tz='UTC', origin='1970-01-01')


== Writing Dataframes ==

As of version 1.0.1, we now support the batch-writing of data.frames to Cassandra.

```

# If you have a Cassandra table "test_table" with schema that looks like this:

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
  
# And you have a data.frame:
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

# ... and that data.frame has NAs                      
myDf1[3,]$float_1 <- NA
myDf1[3,]$str_val <- NA
myDf1[3,]$factor_str_val <- NA
myDf1[4,]$double_1 <- NA
myDf1[4,]$dt_val <- NA
myDf1[5,]$int_1 <- NA
myDf1[5,]$ts_val <- NA

# you can save it like this:
cass_save_df(jCassSess, myDf1, "test_insert_table", row_batches=2)

```

See `cass_save_df` for details.


== Running tests ==

First, you need to run `mkdist` from the command line.  This will create the Java classes needed.

Then in Rstudio you can Build & Reload and run tests


