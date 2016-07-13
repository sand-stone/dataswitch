package slipstream.extractor;

/**
 * This class defines a more raw Filter by the fact that its properties are not
 * automatically set by using setter methods. Implementations must configure
 * their properties manually.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public interface FilterManualProperties extends Filter
{
    /**
     * Set filter's configuration prefix. This is important in order for the
     * filter to be able to know where its properties are in the configuration
     * file.<br/>
     * Eg. of how filter's properties could be read:<br/>
     * <code>
     * TungstenProperties filterProperties = properties.subset(configPrefix
                + ".", true);
     * </code>
     * 
     * @param configPrefix Configuration prefix.
     * @see com.continuent.tungsten.common.config.TungstenProperties#subset(String, boolean)
     */
    public void setConfigPrefix(String configPrefix);
}
