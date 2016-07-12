package slipstream.extractor;

import slipstream.extractor.ReplicatorException;

/**
 * This class defines a ConsistencyException
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alexey Yurchenko</a>
 * @version 1.0
 */
public class ConsistencyException extends ReplicatorException
{

  /**
   * 
   */
  private static final long serialVersionUID = 6105152751419283356L;

  /**
   * Creates a new <code>ConsistencyException</code> object
   * 
   */
  public ConsistencyException()
  {
    super();
  }

  /**
   * Creates a new <code>ConsistencyException</code> object
   * 
   * @param arg0
   */
  public ConsistencyException(String arg0)
  {
    super(arg0);
  }

  /**
   * Creates a new <code>ConsistencyException</code> object
   * 
   * @param arg0
   */
  public ConsistencyException(Throwable arg0)
  {
    super(arg0);
  }

  /**
   * Creates a new <code>ConsistencyException</code> object
   * 
   * @param arg0
   * @param arg1
   */
  public ConsistencyException(String arg0, Throwable arg1)
  {
    super(arg0, arg1);
  }

}
