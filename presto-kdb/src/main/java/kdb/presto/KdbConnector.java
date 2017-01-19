package kdb.presto;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static kdb.presto.KdbTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class KdbConnector
  implements Connector
{
  private static final Logger log = Logger.get(KdbConnector.class);

  private final LifeCycleManager lifeCycleManager;
  private final KdbMetadata metadata;
  private final KdbSplitManager splitManager;
  private final KdbRecordSetProvider recordSetProvider;

  @Inject
  public KdbConnector(
                      LifeCycleManager lifeCycleManager,
                      KdbMetadata metadata,
                      KdbSplitManager splitManager,
                      KdbRecordSetProvider recordSetProvider)
  {
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
    this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
  {
    return INSTANCE;
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
  {
    return metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager()
  {
    return splitManager;
  }

  @Override
  public ConnectorRecordSetProvider getRecordSetProvider()
  {
    return recordSetProvider;
  }

  @Override
  public final void shutdown()
  {
    try {
      lifeCycleManager.stop();
    }
    catch (Exception e) {
      log.error(e, "Error shutting down connector");
    }
  }
}
