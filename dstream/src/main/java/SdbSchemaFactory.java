package dstream;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;

public class SdbSchemaFactory implements SchemaFactory {
  /** Name of the column that is implicitly created in a CSV stream table
   * to hold the data arrival time. */
  static final String ROWTIME_COLUMN_NAME = "ROWTIME";

  /** Public singleton, per factory contract. */
  public static final SdbSchemaFactory INSTANCE = new SdbSchemaFactory();

  private SdbSchemaFactory() {
  }

  public Schema create(SchemaPlus parentSchema, String name,
                       Map<String, Object> operand) {
    final String directory = (String) operand.get("directory");
    final File base =
      (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    File directoryFile = new File(directory);
    if (base != null && !directoryFile.isAbsolute()) {
      directoryFile = new File(base, directory);
    }
    String flavorName = (String) operand.get("flavor");
    SdbTable.Flavor flavor;
    if (flavorName == null) {
      flavor = SdbTable.Flavor.SCANNABLE;
    } else {
      flavor = SdbTable.Flavor.valueOf(flavorName.toUpperCase());
    }
    return new SdbSchema(directoryFile, flavor);
  }
}
