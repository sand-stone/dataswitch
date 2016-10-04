package dstream;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.io.File;
import java.util.Map;

/**
 * Factory that creates a {@link SdbTranslatableTable}.
 *
 * <p>Allows a CSV table to be included in a model.json file, even in a
 * schema that is not based upon {@link SdbSchema}.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class SdbTableFactory implements TableFactory<SdbTable> {
  // public constructor, per factory contract
  public SdbTableFactory() {
  }

  public SdbTable create(SchemaPlus schema, String name,
      Map<String, Object> operand, RelDataType rowType) {
    String fileName = (String) operand.get("file");
    File file = new File(fileName);
    final File base =
        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    if (base != null && !file.isAbsolute()) {
      file = new File(base, fileName);
    }
    final RelProtoDataType protoRowType =
        rowType != null ? RelDataTypeImpl.proto(rowType) : null;
    return new SdbScannableTable(file, protoRowType);
  }
}
