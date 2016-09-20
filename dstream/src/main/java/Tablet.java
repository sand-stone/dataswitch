package dstream;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Tablet implements Closeable {
  private static Logger log = LogManager.getLogger(Tablet.class);
  private Connection conn;
  private String db;
  private final  String dbconfig = "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3), checkpoint=(log_size=2GB,wait=3600)";
  private Table table;

  private static boolean checkDir(String dir) {
    boolean ret = true;
    File d = new File(dir);
    if(d.exists()) {
      if(d.isFile())
        ret = false;
    } else {
      d.mkdirs();
    }
    return ret;
  }

  public Tablet(String location, Table table) {
    checkDir(location);
    this.table = table;
    conn = wiredtiger.open(location, dbconfig);
    Session session = conn.open_session(null);
    session.create("table:"+table.name, getStorage());
    session.close(null);
  }

  private String getColType(Table.Column col) {
    String ret = "";
    switch(col.getType()) {
    case Int8:
      ret = "b";
      break;
    case Int16:
      ret = "h";
      break;
    case Int32:
      ret = "i";
      break;
    case Int64:
      ret = "q";
      break;
    case Float:
      ret = "i";
      break;
    case Double:
      ret = "q";
      break;
    case Varchar:
      ret = "S";
      break;
    case Symbol:
      ret = "S";
      break;
    case Blob:
      ret = "u";
      break;
    case Timestamp:
      ret = "q";
      break;
    case DateTime:
      ret = "i";
      break;
    }
    return ret;
  }

  private String getStorage() {
    StringBuilder key_format = new StringBuilder();
    StringBuilder value_format = new StringBuilder();
    StringBuilder cols = new StringBuilder();
    key_format.append("key_format=");
    value_format.append(",value_format=");
    cols.append("columns=(");
    boolean haskey = false; boolean start = true;
    for(Table.Column col : table.cols) {
      if(col.iskey()) {
        key_format.append(getColType(col));
        haskey = true;
      } else {
        value_format.append(getColType(col));
      }
      if(!start) {
        cols.append(",");
        start = false;
      }
      cols.append(col.getName());
    }
    if(!haskey)
      key_format.append("key_format=r");
    cols.append(")");
    return "(type=lsm," + key_format.toString() + "," + value_format.toString() + "," + cols;
  }

  public void close() {
    conn.close(null);
  }

}
