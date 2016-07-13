package slipstream.extractor;

import slipstream.extractor.TungstenProperties;
import slipstream.extractor.ReplicatorException;
import slipstream.extractor.FilterManualProperties;

/**
 * Specification for a component, including the implementation class and input
 * properties, and utility methods to manage the component life cycle.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PluginSpecification
{
    private final String             prefix;
    private final String             name;
    private final Class<?>           pluginClass;
    private final TungstenProperties properties;

    public PluginSpecification(String prefix, String name,
            Class<?> pluginClass, TungstenProperties properties)
    {
        this.prefix = prefix;
        this.name = name;
        this.pluginClass = pluginClass;
        this.properties = properties;
    }

    public String getPrefix()
    {
        return prefix;
    }

    public String getName()
    {
        return name;
    }

    public Class<?> getPluginClass()
    {
        return pluginClass;
    }

    public TungstenProperties getProperties()
    {
        return properties;
    }

    /**
     * Instantiate the plugin and assign properties. 
     * 
     * @throws PluginException Thrown if instantiation fails
     */
    public ReplicatorPlugin instantiate(int id) throws ReplicatorException
    {
        ReplicatorPlugin plugin = PluginLoader.load(pluginClass.getName());
        if (plugin instanceof FilterManualProperties)
            ((FilterManualProperties) plugin).setConfigPrefix(prefix);
        else
            properties.applyProperties(plugin);
        return plugin;
    }
}
