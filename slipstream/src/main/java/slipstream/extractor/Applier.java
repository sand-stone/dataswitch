package slipstream.extractor;

import slipstream.extractor.ReplicatorException;
import slipstream.extractor.ConsistencyException;
import slipstream.extractor.ReplDBMSEvent;
import slipstream.extractor.ReplDBMSHeader;
import slipstream.extractor.ReplicatorPlugin;

/**
 * Denotes an applier that can process events with full metadata.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @see com.continuent.tungsten.replicator.applier.RawApplier
 */
public interface Applier extends ReplicatorPlugin
{
    /**
     * Apply the proffered event to the replication target.
     * 
     * @param event Event to be applied
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multipart event
     * @param doRollback Boolean flag indicating whether this transaction should
     *            rollback
     * @param syncTHL Should this applier synchronize the trep_commit_seqno
     *            table? This should be false for slave.
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws ConsistencyException Thrown if the applier detects that a
     *             consistency check has failed
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ReplicatorException,
            ConsistencyException, InterruptedException;

    /**
     * Update current recovery position but do not apply an event.
     * 
     * @param header Header containing seqno, event ID, etc.
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multipart event
     * @param syncTHL Should this applier synchronize the trep_commit_seqno
     *            table? This should be false for slave.
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void updatePosition(ReplDBMSHeader header, boolean doCommit,
            boolean syncTHL) throws ReplicatorException, InterruptedException;

    /**
     * Commits current open transaction to ensure data applied up to current
     * point are durable.
     * 
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void commit() throws ReplicatorException, InterruptedException;

    /**
     * Rolls back any current work.
     * 
     * @throws InterruptedException
     */
    public void rollback() throws InterruptedException;

    /**
     * Return header information corresponding to last committed transaction.
     * 
     * @return Header data for last committed transaction
     * @throws ReplicatorException Thrown if getting sequence number fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException;

}
