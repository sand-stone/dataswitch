package slipstream.extractor;

import java.sql.Timestamp;

/**
 * Denotes header data used for replication. This is the core information used
 * to remember the replication position so that restart is possible.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ReplDBMSHeader
{
    /**
     * Returns the log sequence number, a monotonically increasing whole number
     * starting at 0 that denotes a single transaction.
     */
    public long getSeqno();
    
    /**
     * Returns the last sequence number for a filtered event or -1 for other
     * normal events
     */
    public long getLastSeqno();

    /**
     * Returns the event fragment number, a monotonically increasing whole
     * number starting at 0.
     */
    public short getFragno();

    /**
     * Returns true if this fragment is the last one.
     */
    public boolean getLastFrag();

    /**
     * Returns the ID of the data source from which this event was originally
     * extracted.
     */
    public String getSourceId();

    /**
     * Returns the epoch number, a number that identifies a continuous sequence
     * of events from the time a master goes online until it goes offline.
     */
    public long getEpochNumber();

    /**
     * Returns the native event ID corresponding to this log sequence number.
     */
    public String getEventId();

    /**
     * Returns the shard ID for this transaction.
     */
    public String getShardId();

    /**
     * Returns the extractedTstamp value.
     */
    public Timestamp getExtractedTstamp();

    /**
     * Returns the applied latency in seconds.
     */
    public long getAppliedLatency();
    
    /**
     * Not all DBMS types have this field for position, hence might be null.
     */
    public Timestamp getUpdateTstamp();

    /**
     * Not all DBMS types have this field for position, in which case -1 is
     * returned.
     */
    public Long getTaskId();
}
