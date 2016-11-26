import java.sql.*;

public class MySQLDataGen {
  static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  static final String DB_URL = "jdbc:mysql://localhost/acme?autoReconnect=true&useSSL=false";

  static final String USER = "root";
  static final String PASS = "yourpassword";

  private static void inserts(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    int count = 100;

    String sql = "INSERT INTO Employees (first, last, age)";
    for (int i = 0; i < count; i++) {
      String value = " VALUES ('" + ("First" + i) + "', '" + ("Last"+i) +"'," +  i%30 + ")";
      stmt.executeUpdate(sql+value);
    }
    //String sql = "INSERT INTO Employees (first, last, age)" +
    //  "VALUES ('John', 'Doe', 18)";
    stmt.close();
  }

  private static void select(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    String sql;
    sql = "SELECT id, first, last, age FROM Employees";
    ResultSet rs = stmt.executeQuery(sql);

    while(rs.next()){
      int id  = rs.getInt("id");
      int age = rs.getInt("age");
      String first = rs.getString("first");
      String last = rs.getString("last");

      System.out.print("ID: " + id);
      System.out.print(", Age: " + age);
      System.out.print(", First: " + first);
      System.out.println(", Last: " + last);
    }
    rs.close();
    stmt.close();
  }

  public static void main(String[] args) {
    Connection conn = null;
    Statement stmt = null;
    try{
      Class.forName("com.mysql.jdbc.Driver");

      System.out.println("Connecting to database...");
      conn = DriverManager.getConnection(DB_URL,USER,PASS);

      inserts(conn);

      select(conn);

      conn.close();
    }catch(SQLException se){
      se.printStackTrace();
    }catch(Exception e){
      e.printStackTrace();
    }finally{
      try{
        if(stmt!=null)
          stmt.close();
      }catch(SQLException se2){
      }
      try{
        if(conn!=null)
          conn.close();
      }catch(SQLException se){
        se.printStackTrace();
      }
    }
    System.out.println("Goodbye!");
  }
}
