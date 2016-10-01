package dstream;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map;

public class StreamTableSchema extends AbstractSchema {
  private static Logger log = LogManager.getLogger(StreamTableSchema.class);
  final File directoryFile;

  public StreamTableSchema(File directoryFile) {
    super();
    this.directoryFile = directoryFile;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string. */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null. */
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
      ? s.substring(0, s.length() - suffix.length())
      : null;
  }

  @Override protected Map<String, Table> getTableMap() {
    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    File[] files = directoryFile.listFiles(
                                           new FilenameFilter() {
                                             public boolean accept(File dir, String name) {
                                               final String nameSansGz = trim(name, ".gz");
                                               return nameSansGz.endsWith(".csv")
                                                 || nameSansGz.endsWith(".json");
                                             }
                                           });
    if (files == null) {
      System.out.println("directory " + directoryFile + " not found");
      files = new File[0];
    }
    // Build a map from table name to table; each file becomes a table.
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (File file : files) {
      String tableName = trim(file.getName(), ".gz");
      final String tableNameSansJson = trimOrNull(tableName, ".json");
      if (tableNameSansJson != null) {
        //JsonTable table = new JsonTable(file);
        //builder.put(tableNameSansJson, table);
        continue;
      }
      tableName = trim(tableName, ".csv");

      final Table table = null;
      builder.put(tableName, table);
    }
    return builder.build();
  }

}
