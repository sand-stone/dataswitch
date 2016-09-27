package dstream;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class STable extends AbstractTable {
  private static Logger log = LogManager.getLogger(STable.class);

  protected final File file;
  protected final RelProtoDataType protoRowType;
  protected List<SFieldType> fieldTypes;

  /** Creates a SAbstractTable. */
  STable(File file, RelProtoDataType protoRowType) {
    this.file = file;
    this.protoRowType = protoRowType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<SFieldType>();
      return STableEnumerator.deduceRowType((JavaTypeFactory) typeFactory, file,
                                            fieldTypes);
    } else {
      return STableEnumerator.deduceRowType((JavaTypeFactory) typeFactory,
                                            file,
                                            null);
    }
  }

  public enum Flavor {
    SCANNABLE, FILTERABLE, TRANSLATABLE
  }

}
