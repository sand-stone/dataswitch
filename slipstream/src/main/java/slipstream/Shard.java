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

public class Shard {
  private static Logger log = LogManager.getLogger(Shard.class);
  Session session;

  public static boolean checkDir(String dir) {
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

  public Shard(String db) {
    checkDir(db);
    Connection conn =  wiredtiger.open(db, "create");
    session = conn.open_session(null);
  }

  public void write(Object msg) throws IOException {
    log.info("msg {} = {}",msg.getClass(), msg);
    if(msg instanceof MySQLTransactionEvent) {
      MySQLTransactionEvent evt = (MySQLTransactionEvent)msg;
      try {
        session.begin_transaction("isolation=snapshot");
        Cursor c = session.open_cursor("table:acme", null, null);
        evt.putKey(c);
        evt.putValue(c);
        c.insert();
      } finally {
        session.commit_transaction(null);
      }
    }
  }

}
