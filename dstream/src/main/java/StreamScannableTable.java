package dstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamScannableTable extends AbstractTable
  implements StreamableTable, org.apache.calcite.schema.ScannableTable {
  private static Logger log = LogManager.getLogger(StreamScannableTable.class);

  protected final File file;
  protected final RelProtoDataType protoRowType;
  protected List<StreamFieldType> fieldTypes;

  StreamScannableTable(File file, RelProtoDataType protoRowType) {
    log.info("create table {}", file);
    this.file = file;
    this.protoRowType = protoRowType;
  }

  public Statistic getStatistic() {
    return null;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<>();
      return StreamTableEnumerator.deduceRowType((JavaTypeFactory) typeFactory, file, fieldTypes, true);
    } else {
      return StreamTableEnumerator.deduceRowType((JavaTypeFactory) typeFactory, file, null, true);
    }
  }

  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  public String toString() {
    return "StreamScannableTable";
  }

  public Enumerable<Object[]> scan(DataContext root) {
    log.info("scan {}", root);
    final int[] fields = StreamTableEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new StreamTableEnumerator<>(file, cancelFlag, true, null,
                                           new StreamTableEnumerator.ArrayRowConverter(fieldTypes, fields, true));
      }
    };
  }

  @Override public Table stream() {
    log.info("stream {} {}", (this instanceof org.apache.calcite.schema.StreamableTable),
             (this instanceof org.apache.calcite.schema.ScannableTable));
    return this;
  }
}
