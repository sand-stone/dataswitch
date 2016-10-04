package dstream;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for table that reads CSV files.
 */
public abstract class SdbTable extends AbstractTable {
  protected final File file;
  protected final RelProtoDataType protoRowType;
  protected List<SdbFieldType> fieldTypes;

  /** Creates a SdbAbstractTable. */
  SdbTable(File file, RelProtoDataType protoRowType) {
    this.file = file;
    this.protoRowType = protoRowType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<SdbFieldType>();
      return SdbEnumerator.deduceRowType((JavaTypeFactory) typeFactory, file,
          fieldTypes);
    } else {
      return SdbEnumerator.deduceRowType((JavaTypeFactory) typeFactory,
          file,
          null);
    }
  }

  /** Various degrees of table "intelligence". */
  public enum Flavor {
    SCANNABLE, FILTERABLE, TRANSLATABLE
  }
}
