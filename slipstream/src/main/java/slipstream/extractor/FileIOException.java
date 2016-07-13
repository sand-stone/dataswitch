package slipstream.extractor;

/**
 * Implements an unexpected error while operating on a file system.
 */
public class FileIOException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public FileIOException(String msg)
    {
        super(msg);
    }

    public FileIOException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
