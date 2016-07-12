package slipstream.extractor;

import java.io.Serializable;

/**
 * This class stores generic name/value pairs in an easily serializable
 * format.  It provides an standard way to represent metadata and 
 * session variables. 
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ReplOption implements Serializable
{
  private static final long serialVersionUID = 1L;
    
  private String name ="";
  private String value = "";
    
  /**
   * Creates a new <code>StatementDataOption</code> object
   * 
   * @param option
   * @param value 
   */
  public ReplOption(String option, String value)
  {
    this.name = option;
    this.value  = value;
  }

  /**
   * Returns the name value.
   * 
   * @return Returns the name.
   */
  public String getOptionName()
  {
    return name;
  }

  /**
   * Returns the value value.
   * 
   * @return Returns the value.
   */
  public String getOptionValue()
  {
    return value;
  }

  /**
   * {@inheritDoc}
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString()
  {
    return name + " = " + value;
  }
}
