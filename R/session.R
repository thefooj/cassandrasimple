#' Get a Cassandra session.  Returns a com.datastax.driver.core.Session session reference.
#' When you run this, you will get warnings about failed logger loading.
#'
#' @example jCassSess <- get_cass_session('my.ip', 'myCluster', 'myKeyspace')
#'
#' @param connectUrl IP address or hostname.  If the host name points to a DNS record with multiple a-records, all addresses will be used by the Datastax driver.  See: http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Cluster.Builder.html#addContactPoint-java.lang.String-
#' @param clusterName The name of the Cassandra cluster
#' @param clusterKeyspace The name of the Cassandra keyspace in the cluster
#' @export
get_cass_session <- function(connectUrl, clusterName, clusterKeyspace) {

  jarBase <- system.file("java", package='cassandrasimple')
  rJava::.jinit()
  rJava::.jaddClassPath(Sys.glob(paste0(jarBase,"/*.jar")))

  jClusterBuilder = rJava::.jcall('com.datastax.driver.core.Cluster', "Lcom/datastax/driver/core/Cluster$Builder;", 'builder')
  jClusterBuilder = rJava::.jcall(jClusterBuilder,  "Lcom/datastax/driver/core/Cluster$Builder;", 'withClusterName', clusterName)
  jClusterBuilder = rJava::.jcall(jClusterBuilder,  "Lcom/datastax/driver/core/Cluster$Builder;", 'addContactPoint', connectUrl)
  jCluster = rJava::.jcall(jClusterBuilder,  "Lcom/datastax/driver/core/Cluster;", 'build')
  jCassSess = rJava::.jcall(jCluster, 'Lcom/datastax/driver/core/Session;', 'connect', clusterKeyspace)

  return(jCassSess)
}

#' Close down a Cassandra session
#'
#' @param jCassSess Java com.datastax.driver.core.Session session, returned from get_cass_session
#' @export
close_cass_session <- function(jCassSess) {
  rJava::.jcall(jCassSess, "V", 'close')
  return(T)
}