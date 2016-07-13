package slipstream.extractor;

/**
 * Specifies the action to take in the event of a replication failure.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public enum FailurePolicy
{
    /** Cancel replication */
    STOP, 
    /** Warn and continue */
    WARN,
    /** Ignore and continue */
    IGNORE
}
