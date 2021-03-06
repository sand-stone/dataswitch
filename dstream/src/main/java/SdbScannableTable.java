package dstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table based on a CSV file.
 *
 * <p>It implements the {@link ScannableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext)} method.
 */
public class SdbScannableTable extends SdbTable
  implements ScannableTable {
  private static Logger log = LogManager.getLogger(SdbScannableTable.class);

  SdbScannableTable(String file, RelProtoDataType protoRowType) {
    //super(new File(file), protoRowType);
    super(null, protoRowType);
    log.info("scan table ctor");
  }

  SdbScannableTable(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
  }

  public String toString() {
    return "SdbScannableTable";
  }

  public Enumerable<Object[]> scan(DataContext root) {
    log.info("scan");
    final int[] fields = SdbEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new SdbEnumerator<>(file, cancelFlag, false,
                                   null, new SdbEnumerator.ArrayRowConverter(fieldTypes, fields));
      }
    };
  }
}
