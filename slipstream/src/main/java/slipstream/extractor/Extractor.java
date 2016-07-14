package slipstream.extractor;

import slipstream.extractor.ReplicatorException;
import slipstream.extractor.ReplDBMSHeader;
import slipstream.extractor.ReplEvent;
import slipstream.extractor.ReplicatorPlugin;

/**
 * Denotes an extractor that extracts DBMSReplEvents with full metadata.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface Extractor extends ReplicatorPlugin
{
    /**
     * Set the value of the last event called. The extractor is responsible for
     * returning the next event in sequence after this one the next time
     * extract() is called.
     * 
     * @param header Last event we extracted
     * @throws ReplicatorException
     */
    public void setLastEvent(ReplDBMSHeader header) throws ReplicatorException;

    /**
     * Provides a new native event ID that overrides the default value that the
     * extractor would otherwise use. This call is used to position raw
     * extractors at a specific native event ID in the DBMS log. It can be
     * ignored by other extractors that do not read from a data source.
     * 
     * @param eventId Event ID at which to begin extracting
     * @throws ReplicatorException
     */
    public void setLastEventId(String eventId) throws ReplicatorException;

    /**
     * Extract the next available event from the log.
     * 
     * @return next ReplDBMSEvent found in the logs or another ReplEvent
     *         subclass containing control data
     */
    public ReplEvent extract() throws ReplicatorException, InterruptedException;

    /**
     * Returns true if there are additional events that may be read as part of
     * block commit processing.
     */
    public boolean hasMoreEvents();

    /**
     * Returns the last event ID committed in a database from which we are
     * extracting. It is used to help synchronize state between the database and
     * the transaction history log. Values returned from this call must
     * correspond with the last extracted DBMSEvent.eventId as follows:
     * <ol>
     * <li>If the returned value is greater than DBMSEvent.eventId, the database
     * has more recent updates</li>
     * <li>If the returned value is equal to DBMSEvent.eventId, all events have
     * been extracted</li>
     * </ol>
     * It should not be possible to receive a value that is less than the last
     * extracted DBMSEvent.eventId as this implies that the extractor is somehow
     * ahead of the state of the database, which would be inconsistent.
     * 
     * @return A current event ID that can be compared with event IDs in
     *         DBMSEvent or null if this type of extractor does not extract from
     *         a database
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException;
}
