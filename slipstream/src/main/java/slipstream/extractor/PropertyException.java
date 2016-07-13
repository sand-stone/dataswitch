package slipstream.extractor;

public class PropertyException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new exception with a message.
     */
    public PropertyException(String msg)
    {
        super(msg);
    }

    /**
     * Creates a new exception with a message and underlying exception.
     */
    public PropertyException(String msg, Throwable t)
    {
        super(msg, t);
    }
}
