package kdb.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class KdbHandleResolver
  implements ConnectorHandleResolver
{
  @Override
  public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
  {
    return KdbTableLayoutHandle.class;
  }

  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass()
  {
    return KdbTableHandle.class;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass()
  {
    return KdbColumnHandle.class;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass()
  {
    return KdbSplit.class;
  }

  @Override
  public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
  {
    return KdbTransactionHandle.class;
  }
}
