package slipstream;

import com.github.shyiko.mysql.binlog.event.*;
import java.io.*;
import com.wiredtiger.db.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.time.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class MySQLTransactionEvent implements Message {
  private static Logger log = LogManager.getLogger(MySQLTransactionEvent.class);

  long serverid;
  String table;
  String database;
  long timestamp;
  long position;
  TableMapEventData mapEvent;
  EventData cudEvent;

  public static enum MySQLFieldType {
    MYSQL_TYPE_DECIMAL(0),
    MYSQL_TYPE_TINY(1),
    MYSQL_TYPE_SHORT(2),
    MYSQL_TYPE_LONG(3),
    MYSQL_TYPE_FLOAT(4),
    MYSQL_TYPE_DOUBLE(5),
    MYSQL_TYPE_NULL(6),
    MYSQL_TYPE_TIMESTAMP(7),
    MYSQL_TYPE_LONGLONG(8),
    MYSQL_TYPE_INT24(9),
    MYSQL_TYPE_DATE(10),
    MYSQL_TYPE_TIME(11),
    MYSQL_TYPE_DATETIME(12),
    MYSQL_TYPE_YEAR(13),
    MYSQL_TYPE_NEWDATE(14),
    MYSQL_TYPE_VARCHAR(15),
    MYSQL_TYPE_BIT(16),
    MYSQL_TYPE_TIMESTAMP2(17),
    MYSQL_TYPE_DATETIME2(18),
    MYSQL_TYPE_TIME2(19),
    MYSQL_TYPE_JSON(245),
    MYSQL_TYPE_NEWDECIMAL(246),
    MYSQL_TYPE_ENUM(247),
    MYSQL_TYPE_SET(248),
    MYSQL_TYPE_TINY_BLOB(249),
    MYSQL_TYPE_MEDIUM_BLOB(250),
    MYSQL_TYPE_LONG_BLOB(251),
    MYSQL_TYPE_BLOB(252),
    MYSQL_TYPE_VAR_STRING(253),
    MYSQL_TYPE_STRING(254),
    MYSQL_TYPE_GEOMETRY(255);

    private int t;

    MySQLFieldType(int t) {
      this.t = t;
    }

    private static MySQLFieldType[] fixedtypes = new MySQLFieldType[]{MYSQL_TYPE_DECIMAL,
                                                                      MYSQL_TYPE_TINY,
                                                                      MYSQL_TYPE_SHORT,
                                                                      MYSQL_TYPE_LONG,
                                                                      MYSQL_TYPE_FLOAT,
                                                                      MYSQL_TYPE_DOUBLE,
                                                                      MYSQL_TYPE_NULL,
                                                                      MYSQL_TYPE_TIMESTAMP,
                                                                      MYSQL_TYPE_LONGLONG,
                                                                      MYSQL_TYPE_INT24,
                                                                      MYSQL_TYPE_DATE,
                                                                      MYSQL_TYPE_TIME,
                                                                      MYSQL_TYPE_DATETIME,
                                                                      MYSQL_TYPE_YEAR,
                                                                      MYSQL_TYPE_NEWDATE,
                                                                      MYSQL_TYPE_VARCHAR,
                                                                      MYSQL_TYPE_BIT,
                                                                      MYSQL_TYPE_TIMESTAMP2,
                                                                      MYSQL_TYPE_DATETIME2,
                                                                      MYSQL_TYPE_TIME2};
    public static MySQLFieldType map(byte t) {
      MySQLFieldType mt = MYSQL_TYPE_STRING;
      int it = t;
      switch(it) {
      case 245:
        mt = MYSQL_TYPE_JSON;
        break;
      case 246:
        mt = MYSQL_TYPE_NEWDECIMAL;
        break;
      case 247:
        mt = MYSQL_TYPE_ENUM;
      case 248:
        mt = MYSQL_TYPE_SET;
        break;
      case 249:
        mt = MYSQL_TYPE_TINY_BLOB;
        break;
      case 250:
        mt = MYSQL_TYPE_MEDIUM_BLOB;
        break;
      case 251:
        mt = MYSQL_TYPE_LONG_BLOB;
        break;
      case 252:
        mt = MYSQL_TYPE_BLOB;
        break;
      case 253:
        mt = MYSQL_TYPE_VAR_STRING;
        break;
      case 254:
        mt = MYSQL_TYPE_STRING;
        break;
      case 255:
        mt = MYSQL_TYPE_GEOMETRY;
      default:
        return fixedtypes[t];
      }
      return mt;
    }
  }

  public static MySQLTransactionEvent get(Cursor cursor) {
    long serverid = cursor.getKeyLong();
    String database = cursor.getKeyString();
    String table = cursor.getKeyString();
    long timestamp = cursor.getKeyLong();
    long position = cursor.getKeyLong();
    TableMapEventData mapEvent = (TableMapEventData)Serializer.deserialize(ByteBuffer.wrap(cursor.getValueByteArray()));
    EventData cudEvent = (EventData)Serializer.deserialize(ByteBuffer.wrap(cursor.getValueByteArray()));
    return new MySQLTransactionEvent(serverid, database, table, timestamp, position, mapEvent, cudEvent);
  }

  public void putKey(Cursor cursor) {
    cursor.putKeyLong(serverid)
      .putKeyString(database)
      .putKeyString(table)
      .putKeyLong(timestamp)
      .putKeyLong(position);
  }

  public void putValue(Cursor cursor) throws IOException {
    cursor.putValueByteArray(Serializer.serialize(mapEvent).array())
      .putValueByteArray(Serializer.serialize(cudEvent).array());
  }

  public MySQLTransactionEvent(long serverid, String database, String table, long timestamp, long position,
                               TableMapEventData mapEvent, EventData cudEvent) {
    this.serverid = serverid;
    this.database = database;
    this.table = table;
    this.timestamp = timestamp;
    this.position = position;
    this.mapEvent = mapEvent;
    this.cudEvent = cudEvent;
  }

  private byte[] getFieldTypes() {
    return mapEvent.getColumnTypes();
  }

  private String genInsert() {
    WriteRowsEventData evt = (WriteRowsEventData)cudEvent;
    String header = "insert into " + this.database + "." + this.table + " Values(";
    byte[] types = getFieldTypes();
    String ret = "";
    for (Serializable row : evt.getRows()) {
      Serializable[] fields = (Serializable[])row;
      int c = 0;
      String body = "";
      for(Serializable field : fields) {
        MySQLFieldType t = MySQLFieldType.map(types[c]);
        String x = null;
        switch(t) {
        case MYSQL_TYPE_VARCHAR:
          body += "'"+ new String((byte[])field) + "'";
          break;
        default:
          body += field.toString();
        }
        c++;
        if(c < fields.length)
          body += ",";
      }
      ret += header + body + ")";
    }
    return ret;
  }

  public String getSQL() {
    String ret ="";
    if (cudEvent instanceof WriteRowsEventData) {
      ret = genInsert();
    }
    return ret;
  }

  public Kind getKind() {
    return Kind.MySQLTransaction;
  }

  public String toString() {
    return "<"+ "serverid:" + serverid +
      " database: "+ database +" table:" + table + " timestamp:" + timestamp + " position:"+ position +">";
  }

}
