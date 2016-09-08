package slipstream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import com.wiredtiger.db.*;

class MetaDataTable implements AutoCloseable {
  private final static String datasource = "table:datasource";
  private final static String ensemble = "table:ensemble";
  private final static String shard = "table:shard";
  private final static String schema = "table:schema";
  private static final String storage_format = "key_format=S,value_format=u";
  private static final String schema_format = "key_format=SSS,value_format=S";

  private static Logger log = LogManager.getLogger(MetaDataTable.class);

  private Connection conn;

  public MetaDataTable(String db) {
    Util.checkDir(db);
    conn = wiredtiger.open(db, "create");
    Session session = conn.open_session(null);
    int ret = session.create(datasource, storage_format);
    check(ret);
    ret = session.create(ensemble, storage_format);
    check(ret);
    ret = session.create(shard, storage_format);
    check(ret);
    ret = session.create(schema, schema_format);
    check(ret);
    session.close(null);
  }

  private void check(int ret) {
    if(ret != 0)
      throw new RuntimeException("wt DataTable creation error:"+ret);
  }

  public Context getContext(String db) {
    return new Context(db);
  }

  public class Context implements AutoCloseable {
    String db;
    Session session;
    Cursor cursor;
    public Context(String db) {
      this.db = db;
      switch(db) {
      case "schema":
        session = MetaDataTable.this.conn.open_session(null);
        cursor = session.open_cursor(schema, null, null);
        break;
      default:
        break;
      }
    }

    public Object search(Object ... args) {
      Object ret = null;
      switch(db) {
      case "schema":
        cursor
          .putKeyString((String)args[0])
          .putKeyString((String)args[1]);
        if(cursor.search() == 0) {
          ret = cursor.getValueString();
        }
        break;
      default:
        break;
      }
      return ret;
    }

    public void insert(Object... args) {
      switch(db) {
      case "schema":
        cursor
          .putKeyString((String)args[0])
          .putKeyString((String)args[1])
          .putValueString((String)args[2]);
        cursor.insert();
        break;
      default:
        break;
      }
    }

    public void close() {
      cursor.close();
      session.close(null);
    }

  }

  public void close() {
    conn.close(null);
  }

}
