package slipstream.extractor;

import slipstream.extractor.TungstenProperties;
import slipstream.extractor.ReplicatorPlugin;

/**
 * Denotes a plugin that is a free-standing service for replicator
 * pipelines accessible from all stages.  Beyond methods required
 * in the interface, PipelineServices may offer any methods that seem
 * useful to client code. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface PipelineService extends ReplicatorPlugin
{
    /** Gets the storage name. */
    public String getName();

    /** Sets the storage name. */
    public void setName(String name);

    /**
     * Returns status information as a set of named properties.
     */
    public TungstenProperties status();
}
