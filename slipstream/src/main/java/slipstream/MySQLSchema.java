package slipstream;

import java.sql.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class MySQLSchema {
  private static Logger log = LogManager.getLogger(MySQLSchema.class);
  private final static String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  String url;
  String user;
  String password;

  public MySQLSchema(String url, String user, String password) {
    this.url = url;
    this.user = user;
    this.password = password;
  }

  public void get() throws SQLException, ClassNotFoundException {
    Class.forName("com.mysql.jdbc.Driver");
    Connection connection = DriverManager.getConnection(url, user, password);
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet tables = metaData.getTables(null, "public", null, new String[]{"TABLE"});
    try {
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        log.info("table = {}", tableName);
        ResultSet cols = metaData.getColumns(null, "public", tableName, null);
        try {
          while (cols.next()) {
            String columnName = cols.getString("COLUMN_NAME");
            String columnType = cols.getString("TYPE_NAME").toLowerCase();
            log.info("col {}={}", columnName, columnType);
          }
        } finally {
          cols.close();
        }
      }
    } finally {
      tables.close();
    }
  }

  public static void main(String[] args) throws Exception {
    new MySQLSchema("jdbc:mysql://localhost/acme?autoReconnect=true&useSSL=false", "root", "mysql").get();
  }

}
