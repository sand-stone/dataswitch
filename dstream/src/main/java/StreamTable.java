package dstream;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.calcite.sql.parser.SqlParser;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class StreamTable extends AbstractTable {
  private static Logger log = LogManager.getLogger(StreamTable.class);

  protected final File file;
  protected final RelProtoDataType protoRowType;
  protected List<StreamFieldType> fieldTypes;

  StreamTable(File file, RelProtoDataType protoRowType) {
    this.file = file;
    this.protoRowType = protoRowType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<StreamFieldType>();
      return StreamTableEnumerator.deduceRowType((JavaTypeFactory) typeFactory, file,
                                                 fieldTypes);
    } else {
      return StreamTableEnumerator.deduceRowType((JavaTypeFactory) typeFactory,
                                                 file,
                                                 null);
    }
  }

  public enum Flavor {
    SCANNABLE, FILTERABLE, TRANSLATABLE
  }

  public static void main(String[] args) throws Exception {
    String sql = "CREATE TABLE REGISTRATION " +
      "(id INTEGER not NULL, " +
      " first VARCHAR(255), " +
      " last VARCHAR(255), " +
      " age INTEGER, " +
      " PRIMARY KEY ( id ))";
    sql =  "UPDATE Registration " +
      "SET age = 30 WHERE id in (100, 101)";
    SqlParser p = SqlParser.create(sql);
    log.info("sql {}", sql);
    log.info("p {}", p.parseStmt());
  }
}
