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
# Source: local data frame [2 x 5]
# 
#           dt    hr obj_id val_1  val_2
#       (date) (int)  (int) (dbl)  (dbl)
# 1 2017-01-20    13     80 24.33 -34.33
# 2 2017-01-20    24     80 14.44  55.55

stuff2 <- cass_query(jCassSess,"select * from test_table where dt = '2017-01-20' and hr = 13")  
# Source: local data frame [1 x 5]
# 
#          dt    hr obj_id val_1  val_2
#      (date) (int)  (int) (dbl)  (dbl)
#1 2017-01-20    13     80 24.33 -34.33

close_cass_session(jCassSess)
```

To install this, do:

```
library(devtools)
install_github("thefooj/cassandrasimple")
```


