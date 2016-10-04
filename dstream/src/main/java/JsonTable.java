package dstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.File;

/**
 * Table based on a JSON file.
 */
public class JsonTable extends AbstractTable implements ScannableTable {
  private final File file;

  /** Creates a JsonTable. */
  JsonTable(File file) {
    this.file = file;
  }

  public String toString() {
    return "JsonTable";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder().add("_MAP",
                                     typeFactory.createMapType(
                                                               typeFactory.createSqlType(SqlTypeName.VARCHAR),
                                                               typeFactory.createTypeWithNullability(
                                                                                                     typeFactory.createSqlType(SqlTypeName.VARCHAR), true))).build();
  }

  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new JsonEnumerator(file);
      }
    };
  }
}
