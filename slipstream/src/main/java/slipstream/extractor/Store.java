package slipstream.extractor;

import slipstream.extractor.TungstenProperties;
import slipstream.extractor.ReplicatorPlugin;

/**
 * Denotes a storage component that holds replication events.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface Store extends ReplicatorPlugin
{
    /** Gets the storage name. */
    public String getName();

    /** Sets the storage name. */
    public void setName(String name);

    /**
     * Returns the maximum persistently stored sequence number.
     */
    public long getMaxStoredSeqno();

    /**
     * Returns the minimum persistently stored sequence number.
     */
    public long getMinStoredSeqno();

    /**
     * Returns status information as a set of named properties.
     */
    public TungstenProperties status();
}
