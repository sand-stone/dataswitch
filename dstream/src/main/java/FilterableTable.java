package dstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.sql.SqlKind;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class FilterableTable extends StreamTable
    implements org.apache.calcite.schema.FilterableTable {

  
  FilterableTable(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
  }

  public String toString() {
    return "FilterableTable";
  }

  public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    final String[] filterValues = new String[fieldTypes.size()];
    for (final Iterator<RexNode> i = filters.iterator(); i.hasNext();) {
      final RexNode filter = i.next();
      if (addFilter(filter, filterValues)) {
        i.remove();
      }
    }
    final int[] fields = StreamTableEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new StreamTableEnumerator<>(file, cancelFlag, false, filterValues,
            new StreamTableEnumerator.ArrayRowConverter(fieldTypes, fields));
      }
    };
  }

  private boolean addFilter(RexNode filter, Object[] filterValues) {
    if (filter.isA(SqlKind.EQUALS)) {
      final RexCall call = (RexCall) filter;
      RexNode left = call.getOperands().get(0);
      if (left.isA(SqlKind.CAST)) {
        left = ((RexCall) left).operands.get(0);
      }
      final RexNode right = call.getOperands().get(1);
      if (left instanceof RexInputRef
          && right instanceof RexLiteral) {
        final int index = ((RexInputRef) left).getIndex();
        if (filterValues[index] == null) {
          filterValues[index] = ((RexLiteral) right).getValue2().toString();
          return true;
        }
      }
    }
    return false;
  }
}
