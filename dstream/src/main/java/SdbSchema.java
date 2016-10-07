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
  private static Logger log = LogManager.getLogger(SdbSchema.class);

  public SdbSchema() {
    super();
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for(String name : SdbSchemaFactory.get().getTables()) {
      builder.put(name, createTable(name));
    }
    return builder.build();
  }

  private Table createTable(String name) {
    return new SdbScannableTable(name, null);
  }
}
