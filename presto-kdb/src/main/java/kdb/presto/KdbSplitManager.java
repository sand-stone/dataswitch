package kdb.presto;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static kdb.presto.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class KdbSplitManager
  implements ConnectorSplitManager
{
  private final String connectorId;
  private final KdbClient exampleClient;

  @Inject
  public KdbSplitManager(KdbConnectorId connectorId, KdbClient exampleClient)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.exampleClient = requireNonNull(exampleClient, "client is null");
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout)
  {
    KdbTableLayoutHandle layoutHandle = checkType(layout, KdbTableLayoutHandle.class, "layout");
    KdbTableHandle tableHandle = layoutHandle.getTable();
    KdbTable table = exampleClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
    // this can happen if table is removed during a query
    checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

    List<ConnectorSplit> splits = new ArrayList<>();
    for (URI uri : table.getSources()) {
      splits.add(new KdbSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), uri));
    }
    Collections.shuffle(splits);

    return new FixedSplitSource(splits);
  }
}
