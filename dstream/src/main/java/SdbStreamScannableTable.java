package dstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table based on a CSV file.
 *
 * <p>It implements the {@link ScannableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext)} method.
 */
public class SdbStreamScannableTable extends SdbScannableTable
  implements StreamableTable {
  /** Creates a SdbScannableTable. */
  SdbStreamScannableTable(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<>();
      return SdbEnumerator.deduceRowType((JavaTypeFactory) typeFactory, file, fieldTypes, true);
    } else {
      return SdbEnumerator.deduceRowType((JavaTypeFactory) typeFactory, file, null, true);
    }
  }

  public String toString() {
    return "SdbStreamScannableTable";
  }

  public Enumerable<Object[]> scan(DataContext root) {
    final int[] fields = SdbEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new SdbEnumerator<>(file, cancelFlag, true, null,
                                   new SdbEnumerator.ArrayRowConverter(fieldTypes, fields, true));
      }
    };
  }

  @Override public Table stream() {
    return this;
  }
}
