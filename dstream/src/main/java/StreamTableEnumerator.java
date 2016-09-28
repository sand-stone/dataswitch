package dstream;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import org.apache.commons.lang3.time.FastDateFormat;
import com.google.common.base.Throwables;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


class StreamTableEnumerator<E> implements Enumerator<E> {
  private static Logger log = LogManager.getLogger(StreamTableEnumerator.class);

  private final String[] filterValues;
  private final AtomicBoolean cancelFlag;
  private final RowConverter<E> rowConverter;
  private E current;

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

  static {
    TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
  }

  public StreamTableEnumerator(File file, AtomicBoolean cancelFlag,
                               List<StreamFieldType> fieldTypes) {
    this(file, cancelFlag, fieldTypes, identityList(fieldTypes.size()));
  }

  public StreamTableEnumerator(File file, AtomicBoolean cancelFlag,
                               List<StreamFieldType> fieldTypes, int[] fields) {
    //noinspection unchecked
    this(file, cancelFlag, false, null,
         (RowConverter<E>) converter(fieldTypes, fields));
  }

  public StreamTableEnumerator(File file, AtomicBoolean cancelFlag, boolean stream,
                               String[] filterValues, RowConverter<E> rowConverter) {
    log.info("create");
    this.cancelFlag = cancelFlag;
    this.rowConverter = rowConverter;
    this.filterValues = filterValues;
    try {
      if (stream) {

      } else {
        throw new IOException("oops");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static RowConverter<?> converter(List<StreamFieldType> fieldTypes,
                                           int[] fields) {
    if (fields.length == 1) {
      final int field = fields[0];
      return new SingleColumnRowConverter(fieldTypes.get(field), field);
    } else {
      return new ArrayRowConverter(fieldTypes, fields);
    }
  }

  static RelDataType deduceRowType(JavaTypeFactory typeFactory, File file,
                                   List<StreamFieldType> fieldTypes) {
    return deduceRowType(typeFactory, file, fieldTypes, false);
  }

  static RelDataType deduceRowType(JavaTypeFactory typeFactory, File file,
                                   List<StreamFieldType> fieldTypes, Boolean stream) {
    log.info("deduceRowType");
    return null;
  }

  public E current() {
    return current;
  }

  public boolean moveNext() {
    try {
      outer:
      for (;;) {
        if (cancelFlag.get()) {
          return false;
        }
        final String[] strings = null;//reader.readNext();
        if (strings == null) {
          if(true) {//if (reader instanceof CsvStreamReader) {
            try {
              if(true)
                throw new IOException("oops");
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              throw Throwables.propagate(e);
            }
            continue;
          }
          current = null;
          return false;
        }
        if (filterValues != null) {
          for (int i = 0; i < strings.length; i++) {
            String filterValue = filterValues[i];
            if (filterValue != null) {
              if (!filterValue.equals(strings[i])) {
                continue outer;
              }
            }
          }
        }
        current = rowConverter.convertRow(strings);
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    try {
      throw new IOException("oops");
    } catch (IOException e) {
      throw new RuntimeException("Error closing reader", e);
    }
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  /** Row converter. */
  abstract static class RowConverter<E> {
    abstract E convertRow(String[] rows);

    protected Object convert(StreamFieldType fieldType, String string) {
      if (fieldType == null) {
        return string;
      }
      switch (fieldType) {
      case BOOLEAN:
        if (string.length() == 0) {
          return null;
        }
        return Boolean.parseBoolean(string);
      case BYTE:
        if (string.length() == 0) {
          return null;
        }
        return Byte.parseByte(string);
      case SHORT:
        if (string.length() == 0) {
          return null;
        }
        return Short.parseShort(string);
      case INT:
        if (string.length() == 0) {
          return null;
        }
        return Integer.parseInt(string);
      case LONG:
        if (string.length() == 0) {
          return null;
        }
        return Long.parseLong(string);
      case FLOAT:
        if (string.length() == 0) {
          return null;
        }
        return Float.parseFloat(string);
      case DOUBLE:
        if (string.length() == 0) {
          return null;
        }
        return Double.parseDouble(string);
      case DATE:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_DATE.parse(string);
          return new java.sql.Date(date.getTime());
        } catch (ParseException e) {
          return null;
        }
      case TIME:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_TIME.parse(string);
          return new java.sql.Time(date.getTime());
        } catch (ParseException e) {
          return null;
        }
      case TIMESTAMP:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_TIMESTAMP.parse(string);
          return new java.sql.Timestamp(date.getTime());
        } catch (ParseException e) {
          return null;
        }
      case STRING:
      default:
        return string;
      }
    }
  }

  /** Array row converter. */
  static class ArrayRowConverter extends RowConverter<Object[]> {
    private final StreamFieldType[] fieldTypes;
    private final int[] fields;
    //whether the row to convert is from a stream
    private final boolean stream;

    ArrayRowConverter(List<StreamFieldType> fieldTypes, int[] fields) {
      this.fieldTypes = fieldTypes.toArray(new StreamFieldType[fieldTypes.size()]);
      this.fields = fields;
      this.stream = false;
    }

    ArrayRowConverter(List<StreamFieldType> fieldTypes, int[] fields, boolean stream) {
      this.fieldTypes = fieldTypes.toArray(new StreamFieldType[fieldTypes.size()]);
      this.fields = fields;
      this.stream = stream;
    }

    public Object[] convertRow(String[] strings) {
      if (stream) {
        return convertStreamRow(strings);
      } else {
        return convertNormalRow(strings);
      }
    }

    public Object[] convertNormalRow(String[] strings) {
      final Object[] objects = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }

    public Object[] convertStreamRow(String[] strings) {
      final Object[] objects = new Object[fields.length + 1];
      objects[0] = System.currentTimeMillis();
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i + 1] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }
  }

  /** Single column row converter. */
  private static class SingleColumnRowConverter extends RowConverter {
    private final StreamFieldType fieldType;
    private final int fieldIndex;

    private SingleColumnRowConverter(StreamFieldType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    public Object convertRow(String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }
}
