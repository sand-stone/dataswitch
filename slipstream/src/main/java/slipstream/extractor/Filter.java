package slipstream.extractor;

import slipstream.extractor.ReplicatorException;
import slipstream.extractor.ReplDBMSEvent;
import slipstream.extractor.ReplicatorPlugin;

/**
 * 
 * This class defines a Filter
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface Filter extends ReplicatorPlugin
{
    /**
     * Filter the event. Filters may transform the event or return null if the
     * event should be discarded. Filters must be prepared to be interrupted,
     * which mechanism is used to cancel processing.
     * <p>
     * 
     * @param event An event to be filtered
     * @return Filtered ReplDBMSEvent or null
     * @throws ReplicatorException Thrown if there is a processing error
     * @throws InterruptedException Must be thrown if the filter is interrupted
     *             or the replicator may hang
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException;
}
