package slipstream.extractor;

import slipstream.extractor.ReplicatorException;

/**
 * Denotes a catalog entity and specifies the contract for the catalog table
 * life-cycle.
 */
public interface CatalogEntity
{
    /**
     * Complete configuration. This is called after setters are invoked.
     * 
     * @throws ReplicatorException Thrown if configuration is incomplete or
     *             fails
     */
    public void configure() throws ReplicatorException, InterruptedException;

    /**
     * Prepare for use. This method is assumed to allocate any required
     * resources
     * 
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare() throws ReplicatorException, InterruptedException;

    /**
     * Release all resources. This is called before the table is deallocated.
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release() throws ReplicatorException, InterruptedException;

    /**
     * Ensures all catalog data are present and properly initialized for use. If
     * data are absent, this call creates them. If they are present, this call
     * validates that they are ready for use.
     */
    public void initialize() throws ReplicatorException, InterruptedException;

    /**
     * Removes any and all catalog data. This is a dangerous call as it will
     * cause the replication service to lose all memory of its position.
     * 
     * @return True if the catalog data are thought to be removed/absent, false
     *         if an error occurs during deletion
     */
    public boolean clear() throws ReplicatorException, InterruptedException;
}
