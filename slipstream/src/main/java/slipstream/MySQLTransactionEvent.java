package slipstream;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;
import com.wiredtiger.db.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

  public static MySQLTransactionEvent get(Cursor cursor) {
    long serverid = cursor.getKeyLong();
    String database = cursor.getKeyString();
    String table = cursor.getKeyString();
    long timestamp = cursor.getKeyLong();
    long position = cursor.getKeyLong();
    return new MySQLTransactionEvent(serverid, database, table, timestamp, position);
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

  MySQLTransactionEvent(long serverid, String database, String table, long timestamp, long position) {
    this.serverid = serverid;
    this.database = database;
    this.table = table;
    this.timestamp = timestamp;
    this.position = position;
  }

  public Kind getKind() {
    return Kind.MySQLTransaction;
  }

  public String toString() {
    return "<"+ "serverid:" + serverid +
      " database: "+ database +" table:" + table + " timestamp:" + timestamp + " position:"+ position +">";
  }

}
