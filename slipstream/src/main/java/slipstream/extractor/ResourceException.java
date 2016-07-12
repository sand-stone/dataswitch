package slipstream.extractor;

public class ResourceException extends Exception
{

  /**
   * 
   */
  private static final long serialVersionUID = 6387458316241931596L;

  public ResourceException()
  {
       
  }

  public ResourceException(String message)
  {
    super(message);
       
  }

  public ResourceException(Throwable cause)
  {
    super(cause);
       
  }

  public ResourceException(String message, Throwable cause)
  {
    super(message, cause);
       
  }

}
