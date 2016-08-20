package slipstream;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.io.*;
import com.wiredtiger.db.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.time.*;

public class MySQLTransactionEvent implements Message {
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

  public String getSQL() {
    return "sql string" + mapEvent + cudEvent;
  }

  public Kind getKind() {
    return Kind.MySQLTransaction;
  }

  public String toString() {
    return "<"+ "serverid:" + serverid +
      " database: "+ database +" table:" + table + " timestamp:" + timestamp + " position:"+ position +">";
  }

}
