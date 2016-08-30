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
  final static String uri = "table:slipstream";
  
  public Shard(String db) {
    this(db, false);
  }

  public Shard(String db, boolean applier) {
    Util.checkDir(db);
    Connection conn = wiredtiger.open(db, "create");
    session = conn.open_session(null);
    session.create(uri, "type=lsm,"+DBTransactionEvent.StorageFormat);
    if(applier)
      new Thread(new MySQLApplier(conn, uri)).start();
  }

  public void write(Object msg) throws IOException {
    log.info("write into data shard {} = {}",msg.getClass(), msg);
    if(msg instanceof DBTransactionEvent) {
      DBTransactionEvent evt = (DBTransactionEvent)msg;
      try {
        Cursor c = session.open_cursor("table:slipstream", null, null);
        session.begin_transaction("isolation=snapshot");
        evt.putKey(c);
        evt.putValue(c);
        c.insert();
      } finally {
        session.commit_transaction(null);
      }
    }
  }

}
