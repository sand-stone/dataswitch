package slipstream;

import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import com.wiredtiger.db.*;
import java.sql.*;
import org.asynchttpclient.*;
import java.util.concurrent.Future;


public class MySQLApplier implements Runnable {
  private static Logger log = LogManager.getLogger(MySQLApplier.class);
  private final static String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  Session session;
  String uri;

  final String DB_URL = "jdbc:mysql://localhost/acme?autoReconnect=true&useSSL=false";
  String USER = "root";
  String PASS = "mysql";
  private java.sql.Connection conn;

  AsyncHttpClient client;
  String[] urls;

  public MySQLApplier(com.wiredtiger.db.Connection conn, String uri) {
    session = conn.open_session(null);
    this.uri = uri;
    AsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(Integer.MAX_VALUE).build();
    client = new DefaultAsyncHttpClient(config);
    this.urls = new String[]{"http://localhost:10000/schema"};
  }

  private java.sql.Connection getSQLConn() {
    try {
      Class.forName("com.mysql.jdbc.Driver");
      return DriverManager.getConnection(DB_URL,USER,PASS);
    } catch (Exception e) {
      log.info(e);
    }
    return null;
  }

  public String getSchema(String database, String table) {
    Response r = null;
    try {
      r=client.prepareGet(urls[0])
        .addQueryParam("database", database)
        .addQueryParam("table", table)
        .execute()
        .get();
      log.info("r: {}", r);
    } catch(Exception e) {
      log.info(e);
    }
    return r==null? "oops" : r.getResponseBody();
  }

  private void insert(MySQLTransactionEvent evt) throws SQLException {
    //Statement stmt = conn.createStatement();
    log.info("schema: {}", getSchema(evt.database, evt.table));
    log.info("sql string:"+ evt.getSQL(getSchema(evt.database, evt.table)));

    //stmt.close();
  }

  public void run() {
    try {
      Cursor cursor = session.open_cursor(uri, null, null);
      log.info("mysql applier started");
      java.sql.Connection mysqlconn = getSQLConn();
      while(true) {
        Thread.currentThread().sleep(10000);
        cursor.reset();
        while (cursor.next() == 0) {
          MySQLTransactionEvent evt = MySQLTransactionEvent.get(cursor);
          log.info("evt:{}", evt);
          insert(evt);
        }
      }
    } catch(Exception e) {
      log.error(e);
      e.printStackTrace();
    } finally {
    }
  }

}
