package slipstream;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import java.time.*;
import com.google.gson.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.github.shyiko.mysql.binlog.event.*;
import slipstream.replication.proto.Event;
import slipstream.replication.proto.Event.MySQLEventKey;
import slipstream.replication.proto.Event.MySQLEventValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class MySQLChangeRecord {
  private static Logger log = LogManager.getLogger(MySQLChangeRecord.class);

  private final static int keySize = 8+8+8+1;
  long serverid;
  String table;
  String database;
  OperationType op;
  long timestamp;
  long position;

  FieldType[] colsTypes;
  Object[] colsData;
  BitSet includedCols;

  public static enum OperationType {
    None((byte)0),
    Insert((byte)1),
    Update((byte)2),
    Delete((byte)3);

    private byte t;

    OperationType(byte t) {
      this.t = t;
    }

    public byte value() {
      return t;
    }

    public static OperationType map(byte t) {
      switch(t) {
      case (byte)1:
        return Insert;
      case (byte)2:
        return Update;
      case (byte)3:
        return Delete;
      }
      return None;
    }

  }

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

  public MySQLChangeRecord(EventHeaderV4 header, TableMapEventData mapEvent, EventData crudEvent) {
    this.serverid = header.getServerId();
    this.database = mapEvent.getDatabase();
    this.table = mapEvent.getTable();
    this.op = getOpType(crudEvent);
    this.timestamp = header.getTimestamp();
    this.position = header.getNextPosition();
    this.includedCols = getIncludedCols(crudEvent);
    this.colsTypes = getFieldTypes(mapEvent);
    this.colsData = getRows(crudEvent);
  }

  public MySQLChangeRecord(long serverid, String database, String table, OperationType op, long timestamp, long position,
                           BitSet includedCols, FieldType[] colsTypes, Object[] colsData) {
    this.serverid = serverid;
    this.database = database;
    this.table = table;
    this.op = op;
    this.timestamp = timestamp;
    this.position = position;
    this.includedCols = includedCols;
    this.colsTypes = colsTypes;
    this.colsData = colsData;
  }

  private FieldType[] getFieldTypes(TableMapEventData mapEvent) {
    byte[] types = mapEvent.getColumnTypes();
    FieldType[] ftypes = new FieldType[types.length];
    for(int i = 0; i < types.length; i++) {
      ftypes[i] = FieldType.map(types[i]);
    }
    return ftypes;
  }

  private OperationType getOpType(EventData evt) {
    OperationType ret = OperationType.None;
    if (evt instanceof WriteRowsEventData) {
      ret = OperationType.Insert;
    } else if (evt instanceof UpdateRowsEventData) {
      ret = OperationType.Update;
    }
    return ret;
  }

  private Object[] getRows(EventData evt) {
    Object[] rows = null;
    if(evt instanceof UpdateRowsEventData)
      rows = ((UpdateRowsEventData)evt).getRows().toArray();
    else if(evt instanceof WriteRowsEventData)
      rows = ((WriteRowsEventData)evt).getRows().toArray();
    return rows;
  }

  private BitSet getIncludedCols(EventData evt) {
    BitSet ret = null;
    if(evt instanceof UpdateRowsEventData)
      ret = ((UpdateRowsEventData)evt).getIncludedColumns();
    else if(evt instanceof WriteRowsEventData)
      ret = ((WriteRowsEventData)evt).getIncludedColumns();
    return ret;
  }

  public static MySQLChangeRecord get(byte[] key, byte[] value) {
    try {
      MySQLEventKey k = MySQLEventKey.parseFrom(key);
      ByteBuffer kbuf = ByteBuffer.wrap(k.getKeys().toByteArray()).order(ByteOrder.BIG_ENDIAN);
      long serverid = kbuf.getLong();
      long timestamp = kbuf.getLong();
      long position = kbuf.getLong();
      OperationType op = OperationType.map(kbuf.get());

      String database = k.getDatabase();
      String table = k.getTable();

      MySQLEventValue v = MySQLEventValue.parseFrom(value);
      BitSet includedCols = (BitSet)Utils.deserialize(ByteBuffer.wrap(v.getValues(0).toByteArray()));
      FieldType[] colsTypes = (FieldType[])Utils.deserialize(ByteBuffer.wrap(v.getValues(1).toByteArray()));
      Object[] colsData = (Object[])Utils.deserialize(ByteBuffer.wrap(v.getValues(2).toByteArray()));

      return new MySQLChangeRecord(serverid, database, table, op, timestamp, position, includedCols, colsTypes, colsData);
    } catch(InvalidProtocolBufferException e) {}
    return null;
  }

  public byte[] key() {
    byte[] key = ByteBuffer
      .allocate(keySize)
      .order(ByteOrder.BIG_ENDIAN)
      .putLong(serverid)
      .putLong(timestamp)
      .putLong(position)
      .put(op.value())
      .array();
    MySQLEventKey eventK = MySQLEventKey
      .newBuilder()
      .setKeys(ByteString.copyFrom(key))
      .setDatabase(database)
      .setTable(table)
      .build();
    return eventK.toByteArray();
  }

  public byte[] value() {
    try {
      ArrayList<byte[]> values = new ArrayList<byte[]>(3);
      values.add(Utils.serialize(includedCols).array());
      values.add(Utils.serialize(colsTypes).array());
      values.add(Utils.serialize(colsData).array());
      MySQLEventValue eventV = MySQLEventValue
        .newBuilder()
        .addAllValues(values.stream().map(v -> ByteString.copyFrom(v)).collect(toList()))
        .build();
      return eventV.toByteArray();
    } catch(IOException e) {}
    return null;
  }

  private String genInsert(String schema) {
    Gson gson = new Gson();
    LinkedHashMap<String, Object> schemamap = gson.fromJson(schema, LinkedHashMap.class);
    Object[] fields = ((Map)schemamap.get("cols")).entrySet().toArray();
    StringBuilder header = new StringBuilder();
    String database = (String)schemamap.get("database");
    String table = (String)schemamap.get("table");
    header.append("insert ").append(database).append(".").append(table).append(" set ");
    StringBuilder ret = new StringBuilder();
    for(Object row: colsData) {
      Serializable[] datarow = (Serializable[])row;
      StringBuilder body = new StringBuilder();
      for(int i = 0; i < colsTypes.length; i++) {
        if(includedCols.get(i)) {
          String name = ((Map.Entry<String,String>)fields[i]).getKey();
          body.append(name).append("=");
          switch(colsTypes[i]) {
          case VARCHAR:
            body.append("'"+ new String((byte[])datarow[i]) + "'");
            break;
          default:
            body.append(datarow[i].toString());
          }
          if(i < colsTypes.length - 1)
            body.append(",");
        }
      }
      ret.append(header).append(body).append(";");
    }
    return ret.toString();
  }

  private String genUpdate(String schema) {
    StringBuilder ret = new StringBuilder();
    Gson gson = new Gson();
    LinkedHashMap<String, Object> schemamap = gson.fromJson(schema, LinkedHashMap.class);
    Object[] fields = ((Map)schemamap.get("cols")).entrySet().toArray();
    String database = (String)schemamap.get("database");
    String table = (String)schemamap.get("table");
    for (Object chg : colsData) {
      Map.Entry<Serializable[], Serializable[]> row = (Map.Entry<Serializable[], Serializable[]>)chg;
      StringBuilder header = new StringBuilder();
      header.append("update ").append(database).append(".").append(table).append(" set ");
      Serializable[] datarow = (Serializable[])row.getValue();
      StringBuilder body = new StringBuilder();
      for(int i = 0; i < colsTypes.length; i++) {
        if(includedCols.get(i)) {
          String name = ((Map.Entry<String,String>)fields[i]).getKey();
          body.append(name).append("=");
          switch(colsTypes[i]) {
          case VARCHAR:
            body.append("'"+ new String((byte[])datarow[i]) + "'");
            break;
          default:
            body.append(datarow[i].toString());
          }
          if(i < colsTypes.length - 1)
            body.append(",");
        }
      }
      ret.append(header).append(body).append(" where ");
      datarow = (Serializable[])row.getKey();
      body = new StringBuilder();
      for(int i = 0; i < colsTypes.length; i++) {
        if(includedCols.get(i)) {
          String name = ((Map.Entry<String,String>)fields[i]).getKey();
          body.append(name).append("=");
          switch(colsTypes[i]) {
          case VARCHAR:
            body.append("'"+ new String((byte[])datarow[i]) + "'");
            break;
          default:
            body.append(datarow[i].toString());
          }
          if(i < colsTypes.length - 1)
            body.append(" and ");
        }
      }
      ret.append(body).append(";\n");
    }
    return ret.toString();
  }

  public String toSQL(String schema) {
    String ret ="";
    switch(op) {
    case Insert:
      ret = genInsert(schema);
      break;
    case Update:
      ret = genUpdate(schema);
      break;
    default:
      log.info("not supported yet");
      break;
    }
    return ret;
  }

  public String toString() {
    return "<"+ "serverid:" + serverid +
      " database: "+ database +" table:" + table + " timestamp:" + timestamp + " position:"+ position +">";
  }

}
