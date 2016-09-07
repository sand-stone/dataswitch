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

class DataTable {
  final static String datasource = "table:datasource";
  final static String ensemble = "table:ensemble";
  final static String shard = "table:shard";
  private static final String storage_format = "key_format=S,value_format=u";

  private static Logger log = LogManager.getLogger(DataTable.class);  
  Connection conn;

  public DataTable(String db) {
    Util.checkDir(db);
    conn = wiredtiger.open(db, "create");
    Session session = conn.open_session(null);
    int ret = session.create(datasource, storage_format);
    check(ret);
    ret = session.create(ensemble, storage_format);
    check(ret);
    ret = session.create(shard, storage_format);
    check(ret);
    session.close(null);
  }

  private void check(int ret) {
    if(ret != 0)
      throw new RuntimeException("wt DataTable creation error:"+ret);
  }

  public void close() {
    conn.close(null);
  }
  
}
