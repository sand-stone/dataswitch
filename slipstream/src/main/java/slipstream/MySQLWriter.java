package slipstream;

import java.sql.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import kdb.Client;

public class MySQLWriter implements Runnable {
  private static Logger log = LogManager.getLogger(MySQLWriter.class);
  private final static String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  String url;
  String user;
  String password;

  public MySQLWriter(String user, String password) {
    this("jdbc:mysql://localhost/acme?autoReconnect=true&useSSL=false", user, password);
  }

  public MySQLWriter(String url, String user, String password) {
    this.url = url;
    this.user = user;
    this.password = password;
  }

  private Connection getSQLConn() throws Exception {
    Class.forName("com.mysql.jdbc.Driver");
    return DriverManager.getConnection(url, user, password);
  }

  private void process(Client.Result rsp) throws SQLException {
    for(int i = 0; i < rsp.count(); i++) {
      apply(rsp.getKey(i), rsp.getValue(i));
    }
  }

  private void apply(byte[] key, byte[] value) throws SQLException {
    String schema="{\"uri\":\"acme\", \"database\":\"acme\",\"table\":\"employees\", \"cols\":{\"id\":\"int\",\"first\":\"string\",\"last\":\"string\",\"age\":\"int\"}}";
    MySQLChangeRecord record = MySQLChangeRecord.get(key, value);
    log.info("record <{}>", record.toSQL(schema));
    //log.info("sql string:"+ evt.getSQL(getSchema(evt.database, evt.table)));
  }

  public void run() {
    try {
      log.info("mysql writer started");
      Connection mysqlconn = getSQLConn();
      try (Client kclient = new Client("http://localhost:8000", "mysqlevents")) {
        kclient.open();
        Client.Result rsp = kclient.scanFirst(10);
        process(rsp);
        while(rsp.token().length() > 0) {
          rsp = kclient.scanNext(10);
          process(rsp);
        }
      }
      System.exit(0);
    } catch(Exception e) {
      log.error(e);
      e.printStackTrace();
    } finally {
    }
  }

  public static void main(String[] args) throws Exception {
    new MySQLWriter("jdbc:mysql://localhost/acme?autoReconnect=true&useSSL=false", "root", "mysql").run();
  }

}
