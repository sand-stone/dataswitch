package dstream;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class Acme extends StreamTable implements StreamableTable, ScannableTable {
  private static Logger log = LogManager.getLogger(Acme.class);

  Acme(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
    log.info("create table {}", file);
  }

  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  public Statistic getStatistic() {
    return null;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return null;
  }

  public String toString() {
    return "Acme";
  }

  public Enumerable<Object[]> scan(DataContext root) {
    log.info("scan {}", root);
    return null;
  }

  @Override public Table stream() {
    log.info("stream {} {}", (this instanceof org.apache.calcite.schema.StreamableTable),
             (this instanceof org.apache.calcite.schema.ScannableTable));
    return this;
  }
}
