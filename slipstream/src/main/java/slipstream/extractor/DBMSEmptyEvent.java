package slipstream.extractor;

import java.sql.Timestamp;

/**
 * This class defines a DBMSEmptyEvent
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DBMSEmptyEvent extends DBMSEvent
{
    private static final long serialVersionUID = 1300L;

    /**
     * Creates a new empty event.
     * 
     * @param id Event Id
     * @param extractTime Time of commit or failing that extraction
     */
    public DBMSEmptyEvent(String id, Timestamp extractTime)
    {
        super(id, null, extractTime);
    }

    /**
     * Creates a new empty event with the current time as timestamp. WARNING: do
     * not put this type of event into the log as it can mess up parallel
     * replication.
     * 
     * @param id Event Id
     */
    public DBMSEmptyEvent(String id)
    {
        this(id, new Timestamp(System.currentTimeMillis()));
    }
}
