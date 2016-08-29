package slipstream;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.time.*;
import com.wiredtiger.db.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class DBTransactionEvent {
  private static Logger log = LogManager.getLogger(DBTransactionEvent.class);

  long serverid;
  String table;
  String database;
  long timestamp;
  long position;
  FieldType[] colsTypes;
  Object[] colsData;

  public static enum FieldType {
    DECIMAL(0),
    TINY(1),
    SHORT(2),
    LONG(3),
    FLOAT(4),
    DOUBLE(5),
    NULL(6),
    TIMESTAMP(7),
    LONGLONG(8),
    INT24(9),
    DATE(10),
    TIME(11),
    DATETIME(12),
    YEAR(13),
    NEWDATE(14),
    VARCHAR(15),
    BIT(16),
    TIMESTAMP2(17),
    DATETIME2(18),
    TIME2(19),
    JSON(245),
    NEWDECIMAL(246),
    ENUM(247),
    SET(248),
    TINY_BLOB(249),
    MEDIUM_BLOB(250),
    LONG_BLOB(251),
    BLOB(252),
    VAR_STRING(253),
    STRING(254),
    GEOMETRY(255);

    private int t;

    FieldType(int t) {
      this.t = t;
    }

    private static FieldType[] fixedtypes = new FieldType[]{DECIMAL,
                                                            TINY,
                                                            SHORT,
                                                            LONG,
                                                            FLOAT,
                                                            DOUBLE,
                                                            NULL,
                                                            TIMESTAMP,
                                                            LONGLONG,
                                                            INT24,
                                                            DATE,
                                                            TIME,
                                                            DATETIME,
                                                            YEAR,
                                                            NEWDATE,
                                                            VARCHAR,
                                                            BIT,
                                                            TIMESTAMP2,
                                                            DATETIME2,
                                                            TIME2};
    public static FieldType map(byte t) {
      FieldType mt = STRING;
      int it = t;
      switch(it) {
      case 245:
        mt = JSON;
        break;
      case 246:
        mt = NEWDECIMAL;
        break;
      case 247:
        mt = ENUM;
      case 248:
        mt = SET;
        break;
      case 249:
        mt = TINY_BLOB;
        break;
      case 250:
        mt = MEDIUM_BLOB;
        break;
      case 251:
        mt = LONG_BLOB;
        break;
      case 252:
        mt = BLOB;
        break;
      case 253:
        mt = VAR_STRING;
        break;
      case 254:
        mt = STRING;
        break;
      case 255:
        mt = GEOMETRY;
      default:
        return fixedtypes[t];
      }
      return mt;
    }
  }

  public static DBTransactionEvent get(Cursor cursor) {
    long serverid = cursor.getKeyLong();
    String database = cursor.getKeyString();
    String table = cursor.getKeyString();
    long timestamp = cursor.getKeyLong();
    long position = cursor.getKeyLong();
    FieldType[] colsTypes = (FieldType[])Serializer.deserialize(ByteBuffer.wrap(cursor.getValueByteArray()));
    Object[] colsData = (Object[])Serializer.deserialize(ByteBuffer.wrap(cursor.getValueByteArray()));
    return new DBTransactionEvent(serverid, database, table, timestamp, position, colsTypes, colsData);
  }

  public void putKey(Cursor cursor) {
    cursor.putKeyLong(serverid)
      .putKeyString(database)
      .putKeyString(table)
      .putKeyLong(timestamp)
      .putKeyLong(position);
  }

  public void putValue(Cursor cursor) throws IOException {
    cursor.putValueByteArray(Serializer.serialize(colsTypes).array())
      .putValueByteArray(Serializer.serialize(colsData).array());
  }

  public DBTransactionEvent(long serverid, String database, String table, long timestamp, long position,
                            FieldType[] colsTypes, Object[] colsData) {
    this.serverid = serverid;
    this.database = database;
    this.table = table;
    this.timestamp = timestamp;
    this.position = position;
    this.colsTypes = colsTypes;
    this.colsData = colsData;
  }


}
