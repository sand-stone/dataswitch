package slipstream.extractor;

import java.io.*;

public class ConfigurationException extends Exception implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public ConfigurationException(String description)
    {
        super(description);
    }

}
