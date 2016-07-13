package slipstream.extractor;

import slipstream.extractor.ReplicatorException;

/**
 * 
 * This class defines a PluginException
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class PluginException extends ReplicatorException
{

    private static final long serialVersionUID = 1L;

    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param msg
     */
    public PluginException(String msg)
    {
        super(msg);
    }

    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param throwable
     */
    public PluginException(Throwable throwable)
    {
        super(throwable);
    }
    
    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param msg
     * @param throwable
     */
    public PluginException(String msg, Throwable throwable)
    {
        super(msg, throwable);
    }
    
}
