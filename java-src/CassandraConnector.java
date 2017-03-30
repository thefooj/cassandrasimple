package com.tioscapital.cassandrasimple;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.HostDistance;

public class CassandraConnector {

  public static com.datastax.driver.core.Session getConnection(String connectionUrl, String clusterName, String clusterKeyspace, boolean debug) {

    if (debug) System.out.println("CassandraConnector: getConnection -- Got session...");

    /* one connection only for simplicity */
    //PoolingOptions poolingOpts = new PoolingOptions();
    //poolingOpts.setConnectionsPerHost(HostDistance.LOCAL, 1, 256);
    //poolingOpts.setConnectionsPerHost(HostDistance.REMOTE, 1, 256);
    // .withPollingOptions(pollingOpts)


    Cluster cluster = Cluster.builder()
      .withClusterName(clusterName)
      .addContactPoint(connectionUrl)
      .build();

    //System.out.println("max_connections_per_host(LOCAL):" + cluster.getConfiguration().getPoolingOptions().getMaxConnectionsPerHost(HostDistance.LOCAL));
    //System.out.println("max_connections_per_host(REMOTE):" + cluster.getConfiguration().getPoolingOptions().getMaxConnectionsPerHost(HostDistance.REMOTE));
    //System.out.println("max_queue_size" + cluster.getConfiguration().getPoolingOptions().getMaxQueueSize());

    if (debug) System.out.println("CassandraConnector: getConnection -- cluster setup complete.  Getting session...");

    Session sess = cluster.connect(clusterKeyspace);

    if (debug)  System.out.println("CassandraConnector: getConnection -- Got session...");

    return(sess);
  }
}