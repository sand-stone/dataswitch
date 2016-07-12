package slipstream.extractor;

/**
 * Denotes an unexpected error in server processing. The current operation
 * cannot continue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ServerRuntimeException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>ServerRuntimeException</code> object
     * 
     * @param msg Message describing the problem
     */
    public ServerRuntimeException(String msg)
    {
        super(msg);
    }

    /**
     * Creates a new <code>ServerRuntimeException</code> object
     * 
     * @param msg Message describing the problem
     * @param cause Root cause of the exception
     */
    public ServerRuntimeException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
