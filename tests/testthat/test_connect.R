library(cassandrasimple)
library(dplyr)

# first, we must have Cassandra running locally, with keyspace 'cassandrasimple_test'
#  Start cassandra with
#     cd ~/dev/cassandra/apache-cassandra-3.10/bin
#     ./cassandra
#  Start CQLSH:
#     cd ~/dev/cassandra/apache-cassandra-3.10/bin
#     ./cqlsh
#  Set up keyspace:
#     create keyspace cassandrasimple_test with replication = {'class':'SimpleStrategy', 'replication_factor': 1};


test_that("connections work", {
  jCassSess = tryCatch({

    get_cass_session('localhost', 'Test Cluster', 'cassandrasimple_test')

  }, error = function(e) {
    cat("ERROR with Cassandra connection.  Be sure to have a local Cassandra running with cluster 'Test Cluster' and keyspace 'cassandrasimple_test'\n")
    cat("Suppose you have Cassandra downloaded in ~/dev/cassandra/apache-cassandra-3.10/bin\n")
    cat("Then run cassandra with:\n")
    cat("   cd ~/dev/cassandra/apache-cassandra-3.10/bin && ./cassandra\n")
    cat("Start CQLSH:\n")
    cat("   cd ~/dev/cassandra/apache-cassandra-3.10/bin && ./cqlsh\n")
    cat("Create the keyspace in CQLSH:\n")
    cat("   create keyspace cassandrasimple_test with replication = {'class':'SimpleStrategy', 'replication_factor': 1};\n")
  })

  close_cass_session(jCassSess)
})
