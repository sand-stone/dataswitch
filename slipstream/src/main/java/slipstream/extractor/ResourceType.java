package slipstream.extractor;

import java.io.Serializable;

public enum ResourceType implements Serializable
{
    ROOT, /* the root resource of any resource tree */
    EVENT, /* any application */
    CLUSTER, /* a cluster */
    MANAGER, /* a manager of the cluster */
    MEMBER, /* a cluster member */
    FOLDER, /* a general purpose folder */
    QUEUE, /* The resource represents an instance of a queue */
    CONFIGURATION, /*
                    * any type of configuration that can be represented as
                    * properties
                    */
    PROCESS, /* a JVM/MBean server */
    RESOURCE_MANAGER, /*
                       * a class that is exported as a JMX MBean for a specific
                       * component
                       */
    POLICY_MANAGER, /* represents a policy manager */
    OPERATION, /* an operation exported by a JMX MBean */
    DATASOURCE, /* a sql-router datasource */
    MONITOR, DATASERVER, /* a database server */
    HOST, /* a node in a cluster */
    SQLROUTER, /* a sql-router component */
    REPLICATOR, /* a replicator component */
    REPLICATION_SERVICE, /* a single service in a replicator */
    SERVICE_MANAGER, /* a tungsten-manager */
    SERVICE, DIRECTORY, /* a Directory instance */
    DIRECTORY_SESSION, UNDEFINED, EXTENSION, NONE, ANY
    /* any resource */
}
