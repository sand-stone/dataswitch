package slipstream.extractor;

import slipstream.extractor.ReplicatorException;

/**
 * 
 * This class defines a PluginLoader
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class PluginLoader
{
    /**
     * Load plugin implementation.
     * 
     * @param name The name of the plugin implementation class to be loaded.
     * @return new plugin
     * @throws ReplicatorException
     */
    static public ReplicatorPlugin load(String name) throws ReplicatorException
    {
        if (name == null)
            throw new PluginException("Unable to load plugin with null name");
        try
        {
            return (ReplicatorPlugin) Class.forName(name).newInstance();
        }
        catch (Exception e)
        {
            throw new PluginException(e);
        }
    }

    /**
     * Load plugin class.
     * 
     * @param name The name of the plugin implementation class to be loaded.
     * @return new plugin class
     * @throws ReplicatorException
     */
    static public Class<?> loadClass(String name) throws ReplicatorException
    {
        if (name == null)
            throw new PluginException("Unable to load plugin with null name");
        try
        {
            return (Class<?>) Class.forName(name);
        }
        catch (Exception e)
        {
            throw new PluginException(e);
        }
    }
}
