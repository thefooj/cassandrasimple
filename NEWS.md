
## Change in Version 0.2.2 date 2018-06-19

Added support for boolean/logical type

## Changes in Version 0.2.1 date 2017-07-21

Added sleep time option to cass_save_df to sleep between batches


## Changes in Version 0.2.0 date 2017-03-30

Significant speed improvements.  Moved data fetching code out of rJava/JNI to pure Java.
In Java, we build up columnar data, and then quickly build up the data.frame from the list of columns.

Sample runtime improvements:


A query with 4446 rows:

```
[JavaBasedFaster... START @ 2017-03-30 14:15:47 ]
[JavaBasedFaster... STOP @ 2017-03-30 14:15:47 ] - got  4446  rows.

[rJavaSlower... START @ 2017-03-30 14:15:47 ]
[rJavaSlower... STOP @ 2017-03-30 14:15:58 ] - got  4446  rows.
```

A query with 177989 rows:

```
[JavaBasedFaster... START @ 2017-03-30 14:15:58 ]
[JavaBasedFaster... STOP @ 2017-03-30 14:16:03 ] - got  177989  rows.

[rJavaSlower... START @ 2017-03-30 14:16:03 ]
[rJavaSlower... STOP @ 2017-03-30 14:25:01 ] - got  177989  rows.
```




## Changes in Version 0.1.2 date 2017-02-14

Close down cluster connection in addition to session connection

## Changes in Version 0.1.1 date 2017-02-22

We added the capability to save a data.frame to Cassandra. via `cass_save_df`

## Initial Releave Version 0.1.0

This was our initial release
