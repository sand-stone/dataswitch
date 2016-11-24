package replication;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.time.*;
import com.wiredtiger.db.*;
import com.google.gson.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class MySQLChangeRecord {
  private static Logger log = LogManager.getLogger(MySQLChangeRecord.class);

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

  private String genInsert(String schema) {
    Gson gson = new Gson();
    LinkedHashMap<String, String> cols = gson.fromJson(schema, LinkedHashMap.class);
    Object[] fields = cols.entrySet().toArray();
    StringBuilder header = new StringBuilder();
    header.append("insert ").append(this.database).append(".").append(this.table).append(" set ");
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
    LinkedHashMap<String, String> cols = gson.fromJson(schema, LinkedHashMap.class);
    Object[] fields = cols.entrySet().toArray();
    for (Object chg : colsData) {
      Map.Entry<Serializable[], Serializable[]> row = (Map.Entry<Serializable[], Serializable[]>)chg;
      StringBuilder header = new StringBuilder();
      header.append("update ").append(this.database).append(".").append(this.table).append(" set ");
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

  public String getSQL(String schema) {
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
