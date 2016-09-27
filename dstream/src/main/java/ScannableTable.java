package dstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScannableTable extends StreamTable                                    
  implements org.apache.calcite.schema.ScannableTable {
  private static Logger log = LogManager.getLogger(ScannableTable.class);
  
  ScannableTable(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
  }

  public String toString() {
    return "ScannableTable";
  }

  public Enumerable<Object[]> scan(DataContext root) {
    final int[] fields = StreamTableEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new StreamTableEnumerator<>(file, cancelFlag, false,
                                           null, new StreamTableEnumerator.ArrayRowConverter(fieldTypes, fields));
      }
    };
  }
}
