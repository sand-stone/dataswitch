package dstream;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map;
import java.util.List;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class SdbSchema extends AbstractSchema {
  private static Logger log = LogManager.getLogger(SdbSchemaFactory.class);
  private SdbSchemaFactory factory;

  public SdbSchema(SdbSchemaFactory factory) {
    super();
    this.factory = factory;
  }

  @Override protected Map<String, Table> getTableMap() {
    log.info("get Table map");
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    List<String> tableNames = Tablet.getTables("./datanode");
    for(Map.Entry<String, Tablet> entry : factory.getShards().entrySet()) {
      builder.put(entry.getKey(), createTable(entry.getValue()));
    }
    return builder.build();
  }

  private Table createTable(Tablet tablet) {
    log.info("tablet {}", tablet);
    return new SdbScannableTable((String)null, null);
  }
}
