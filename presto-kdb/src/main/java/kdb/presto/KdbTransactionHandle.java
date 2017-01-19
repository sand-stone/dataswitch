package kdb.presto;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public enum KdbTransactionHandle
  implements ConnectorTransactionHandle
{
  INSTANCE
}
