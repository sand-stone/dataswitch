package kdb.presto;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

import java.net.MalformedURLException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class KdbRecordSet
        implements RecordSet
{
    private final List<KdbColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ByteSource byteSource;

    public KdbRecordSet(KdbSplit split, List<KdbColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KdbColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        try {
            byteSource = Resources.asByteSource(split.getUri().toURL());
        }
        catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KdbRecordCursor(columnHandles, byteSource);
    }
}
