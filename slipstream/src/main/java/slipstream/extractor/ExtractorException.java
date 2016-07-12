package slipstream.extractor;

import slipstream.ReplicatorException;

/**
 * This class defines a ExtractorException
 *
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ExtractorException extends ReplicatorException
{
  static final long    serialVersionUID = 1L;
  private final String eventId;

  /**
   * Creates a new exception with only a message.
   *
   * @param msg
   */
  public ExtractorException(String msg)
  {
    this(msg, null, null);
  }

  /**
   * Creates a new exception with only a cause but no message.
   *
   * @param t exception to link cause to
   */
  public ExtractorException(Throwable t)
  {
    this(null, t, null);
  }

  /**
   * Creates a new exception with message and cause,
   *
   * @param msg
   * @param cause
   */
  public ExtractorException(String msg, Throwable cause)
  {
    this(msg, cause, null);
  }

  /**
   * Creates a new exception with message, cause, and associated native
   * eventId.
   */
  public ExtractorException(String msg, Throwable cause, String eventId)
  {
    super(msg, cause);
    this.eventId = eventId;
  }

  public String getEventId()
  {
    return eventId;
  }
}
