package dstream;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Type of a field in a CSV file.
 *
 * <p>Usually, and unless specified explicitly in the header row, a field is
 * of type {@link #STRING}. But specifying the field type in the header row
 * makes it easier to write SQL.</p>
 */
enum SdbFieldType {
  STRING(String.class, "string"),
  BOOLEAN(Primitive.BOOLEAN),
  BYTE(Primitive.BYTE),
  CHAR(Primitive.CHAR),
  SHORT(Primitive.SHORT),
  INT(Primitive.INT),
  LONG(Primitive.LONG),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DATE(java.sql.Date.class, "date"),
  TIME(java.sql.Time.class, "time"),
  TIMESTAMP(java.sql.Timestamp.class, "timestamp");

  private final Class clazz;
  private final String simpleName;

  private static final Map<String, SdbFieldType> MAP =
  new HashMap<String, SdbFieldType>();

  static {
    for (SdbFieldType value : values()) {
      MAP.put(value.simpleName, value);
    }
  }

  SdbFieldType(Primitive primitive) {
    this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
  }

  SdbFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    return typeFactory.createJavaType(clazz);
  }

  public static SdbFieldType of(String typeString) {
    return MAP.get(typeString);
  }
}
