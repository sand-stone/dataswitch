package slipstream.extractor;

import slipstream.extractor.Replicator;
import slipstream.extractor.ResourceState;
import slipstream.extractor.ResourceType;
import slipstream.extractor.TungstenProperties;

public class ReplicatorNotification extends ClusterResourceNotification
{

    /**
     * Used to determine if a given de-serialized object is compatible with this
     * class version.<br>
     * This value must be changed if and only if the new version of this class
     * is not compatible with old versions. See <a
     * href=http://java.sun.com/products/jdk/1.1/docs/guide
     * /serialization/spec/version.doc.html/> for a list of compatible changes.
     */
    private static final long   serialVersionUID               = -2097111546144612171L;

    private static final String REPLICATOR_STATE_ONLINE        = "ONLINE";
    private static final String REPLICATOR_STATE_STOPPED       = "STOPPED";
    private static final String REPLICATOR_STATE_OFFLINE       = "OFFLINE";
    private static final String REPLICATOR_STATE_ERROR         = "OFFLINE:ERROR";
    private static final String REPLICATOR_STATE_BACKUP        = "OFFLINE:BACKUP";
    private static final String REPLICATOR_STATE_RESTORING     = "OFFLINE:RESTORING";
    private static final String REPLICATOR_STATE_SYNCHRONIZING = "SYNCHRONIZING";

    /**
     * @param clusterName
     * @param memberName
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param resourceProps
     */
    public ReplicatorNotification(String clusterName, String memberName,
            String notificationSource, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.REPLICATOR, resourceName,
                resourceState, resourceProps);
    }

    /**
     * Parses the given state provided by the replicator and guess a generic
     * resourceState from it
     * 
     * @param state the state of the connected replicator, as provided by its
     *            JMX state attribute
     * @return a resource state, one of {@link ResourceState}
     */
    static public ResourceState replicatorStateToResourceState(String state)
    {
        if (state.startsWith(REPLICATOR_STATE_ONLINE))
            return ResourceState.ONLINE;
        else if (state.startsWith(REPLICATOR_STATE_STOPPED))
        {
            return ResourceState.STOPPED;
        }
        else if (state.startsWith(REPLICATOR_STATE_OFFLINE))
        {
            if (state.equals(REPLICATOR_STATE_ERROR))
            {
                return ResourceState.SUSPECT;
            }
            else if (state.equals(REPLICATOR_STATE_BACKUP))
            {
                return ResourceState.BACKUP;
            }
            else if (state.equals(REPLICATOR_STATE_RESTORING))
            {
                return ResourceState.RESTORING;
            }
            else
            {
                return ResourceState.OFFLINE;
            }
        }
        else if (state.contains(REPLICATOR_STATE_SYNCHRONIZING))
            return ResourceState.SYNCHRONIZING;
        else
            return ResourceState.UNKNOWN;
    }

    public String getRole()
    {
        return resourceProps.getString(Replicator.ROLE);
    }

    public String getMasterReplicator()
    {
        String masterUri = resourceProps
                .getString(Replicator.MASTER_CONNECT_URI);
        return masterUri.substring(masterUri.indexOf("//") + 2,
                masterUri.lastIndexOf("/"));
    }
}
