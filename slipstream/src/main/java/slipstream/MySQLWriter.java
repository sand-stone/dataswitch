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

  private void process(Statement stmt, Client.Result rsp) throws SQLException {
    for(int i = 0; i < rsp.count(); i++) {
      apply(stmt, rsp.getKey(i), rsp.getValue(i));
    }
  }

  private void apply(Statement stmt, byte[] key, byte[] value) throws SQLException {
    String schema="{\"uri\":\"acme\", \"database\":\"acme\",\"table\":\"employees2\", \"cols\":{\"id\":\"int\",\"first\":\"string\",\"last\":\"string\",\"age\":\"int\"}}";
    MySQLChangeRecord record = MySQLChangeRecord.get(key, value);
    log.info("record <{}>", record.toSQL(schema));
    stmt.executeUpdate(record.toSQL(schema));
  }

  public void run() {
    try {
      log.info("mysql writer started");
      Connection conn = getSQLConn();
      try(Statement stmt = conn.createStatement()) {
        try (Client kclient = new Client("http://localhost:8000", "mysqlevents")) {
          kclient.open();
          Client.Result rsp = kclient.scanFirst(10);
          process(stmt, rsp);
          while(rsp.token().length() > 0) {
            rsp = kclient.scanNext(10);
            process(stmt, rsp);
          }
        }
        System.exit(0);
      }
    } catch(Exception e) {
      log.error(e);
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    new MySQLWriter("jdbc:mysql://localhost/acme?autoReconnect=true&useSSL=false", "root", "mysql").run();
  }

}
