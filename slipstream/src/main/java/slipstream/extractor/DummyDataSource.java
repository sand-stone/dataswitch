package slipstream.extractor;

import slipstream.extractor.ReplicatorException;

/**
 * Implements a dummy data source, which may be used as a stub for DBMS types
 * for which no data source implementation is currently available.
 */
public class DummyDataSource extends AbstractDataSource
        implements
            UniversalDataSource
{
    /** Create new instance. */
    public DummyDataSource()
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#configure()
     */
    public void configure() throws ReplicatorException, InterruptedException
    {
        super.configure();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#prepare()
     */
    @Override
    public void prepare() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * Release all data source tables.
     */
    @Override
    public void release() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * Ensure all tables are ready for use, creating them if necessary.
     */
    @Override
    public void initialize() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     */
    public void reduce() throws ReplicatorException, InterruptedException
    {
        // Do nothing.
    }

    @Override
    public boolean clear() throws ReplicatorException, InterruptedException
    {
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getCommitSeqno()
     */
    @Override
    public CommitSeqno getCommitSeqno()
    {
        throw new UnsupportedOperationException(
                "Dummy data sources do not support catalog operations");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getConnection()
     */
    public UniversalConnection getConnection() throws ReplicatorException
    {
        throw new UnsupportedOperationException(
                "Dummy data sources do not support connection operations");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#releaseConnection(com.continuent.tungsten.replicator.datasource.UniversalConnection)
     */
    public void releaseConnection(UniversalConnection conn)
    {
        throw new UnsupportedOperationException(
                "Dummy data sources do not support connection operations");
    }
}
