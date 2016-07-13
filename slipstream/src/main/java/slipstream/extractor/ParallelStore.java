package slipstream.extractor;

import slipstream.extractor.ReplDBMSHeader;
import slipstream.extractor.WatchPredicate;

/**
 * Denotes a storage component that partitions transactions into disjoint sets.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ParallelStore extends Store
{
    /** Returns the maximum size of individual queues. */
    public void setMaxSize(int size);

    /** Sets the number of queue partitions, i.e., channels. */
    public void setPartitions(int partitions);

    /** Returns the number of partitions for events, i.e., channels. */
    public int getPartitions();

    /** Returns the class used for partitioning transactions across queues. */
    public String getPartitionerClass();

    /** Sets the class used for partitioning transactions across queues. */
    public void setPartitionerClass(String partitionerClass);

    /** Returns the number of events between sync intervals. */
    public int getSyncInterval();

    /**
     * Sets the number of events to process before generating an automatic
     * control event if sync is enabled.
     */
    public void setSyncInterval(int syncInterval);

    /** Returns the maximum number of seconds to do a clean shutdown. */
    public int getMaxOfflineInterval();

    /** Sets the maximum number of seconds for a clean shutdown. */
    public void setMaxOfflineInterval(int maxOfflineInterval);

    /**
     * Inserts stop control event after next complete transaction.
     */
    public void insertStopEvent() throws InterruptedException;

    /**
     * Inserts watch synchronization event after next complete transaction that
     * matches the provided predicate.
     */
    public void insertWatchSyncEvent(WatchPredicate<ReplDBMSHeader> predicate)
            throws InterruptedException;
}
