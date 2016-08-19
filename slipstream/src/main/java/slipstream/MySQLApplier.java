package slipstream;


import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import com.wiredtiger.db.*;

public class MySQLApplier implements Runnable {
  private static Logger log = LogManager.getLogger(MySQLApplier.class);
  
  Session session;
  String uri;
  
  public MySQLApplier(Connection conn, String uri) {
    session = conn.open_session(null);
    this.uri = uri;
  }

  public void run() {
    try {
      Cursor cursor = session.open_cursor(uri, null, null);
      log.info("mysql applier started");
      while(true) {
        Thread.currentThread().sleep(1000);
        cursor.reset();
        while (cursor.next() == 0) {
          MySQLTransactionEvent evt = MySQLTransactionEvent.get(cursor);
          log.info("evt:{}", evt);
        }
      }
    } catch(Exception e) {
      log.info(e);
    } finally {
    }
  }

}
