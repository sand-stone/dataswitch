package kdb.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static kdb.presto.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KdbRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;

    @Inject
    public KdbRecordSetProvider(KdbConnectorId connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "partitionChunk is null");
        KdbSplit exampleSplit = checkType(split, KdbSplit.class, "split");
        checkArgument(exampleSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<KdbColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add(checkType(handle, KdbColumnHandle.class, "handle"));
        }

        return new KdbRecordSet(exampleSplit, handles.build());
    }
}
