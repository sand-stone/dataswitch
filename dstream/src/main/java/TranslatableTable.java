package dstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;

import java.io.File;
import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicBoolean;

public class TranslatableTable extends StreamScannableTable
  implements QueryableTable, org.apache.calcite.schema.TranslatableTable {

  TranslatableTable(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
  }

  public String toString() {
    return "TranslatableTable";
  }

  /** Returns an enumerable over a given projection of the fields.
   *
   * <p>Called from generated code. */
  public Enumerable<Object> project(final DataContext root,
                                    final int[] fields) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return new StreamTableEnumerator<>(file, cancelFlag, fieldTypes, fields);
      }
    };
  }

  public Expression getExpression(SchemaPlus schema, String tableName,
                                  Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  public Type getElementType() {
    return Object[].class;
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  public RelNode toRel(
                       RelOptTable.ToRelContext context,
                       RelOptTable relOptTable) {
    // Request all fields.
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] fields = StreamTableEnumerator.identityList(fieldCount);
    return new StreamTableScan(context.getCluster(), relOptTable, this, fields);
  }
}
