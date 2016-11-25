package slipstream;

import java.sql.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class MySQLWriter implements Runnable {
  private static Logger log = LogManager.getLogger(MySQLWriter.class);
  private final static String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  String url;
  String user;
  String password;

  public MySQLWriter(String url, String user, String password) {
    this.url = url;
    this.user = user;
    this.password = password;
  }

  private Connection getSQLConn() throws Exception {
    Class.forName("com.mysql.jdbc.Driver");
    return DriverManager.getConnection(url, user, password);
  }

  private void insert(Object evt) throws SQLException {
    //log.info("sql string:"+ evt.getSQL(getSchema(evt.database, evt.table)));
  }

  public void run() {
    try {
      log.info("mysql writer started");
      Connection mysqlconn = getSQLConn();
      while(true) {
        insert(null);
      }
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
