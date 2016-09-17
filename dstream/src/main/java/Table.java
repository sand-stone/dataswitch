package dstream;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public final class Table {
  private static Logger log = LogManager.getLogger(Table.class);
  List<Column> cols;

  public enum ColumnType {
    Int8, Int16, Int32, Int64, Float, Double,
    Varchar, Symbol, Timestamp, DateTime,
  }

  public static class Column {
    String name;
    ColumnType type;
    boolean iskey;

    public String getName() {
      return name;
    }

    public ColumnType getType() {
      return type;
    }

    public boolean iskey() {
      return iskey;
    }
  }

  public static class ColumnBuilder {
    private Column c;

    public ColumnBuilder() {
      c = new Column();
    }

    public Column column(){
      return c;
    }

    public void name(String name){
      c.name = name;
    }

    public void type(ColumnType type) {
      c.type = type;
    }

    public void key(boolean iskey) {
      c.iskey = iskey;
    }

    public void value(Object o) {
    }
  }

  public static class TableBuilder {
    Table t;

    public TableBuilder() {
      t = new Table();
    }

    public static Table Table(Consumer<TableBuilder> consumer) {
      TableBuilder builder = new TableBuilder();
      consumer.accept(builder);
      return builder.t;
    }

    public void column(Consumer<ColumnBuilder> consumer){
      ColumnBuilder builder = new ColumnBuilder();
      consumer.accept(builder);
      Column c = builder.column();
      t.addColumn(c);
    }
  }

  public void addColumn(Column c) {
    cols.add(c);
  }

  private Table() {
    cols = new ArrayList<Column>();
  }

  public static Table Table(Column... cols) {
    Table tbl = new Table();
    for(Column c : cols) {
      tbl.addColumn(c);
    }
    return tbl;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("table<");
    for(Column c : cols) {
      builder.append(c.getName());
      builder.append("::"+c.iskey());
      builder.append("::");
      builder.append(c.getType());
      builder.append(" ");
    }
    builder.append(">");
    return builder.toString();
  }

  public static void main(String[] args) {
    Table tbl = TableBuilder.Table(t -> {
        t.column( c -> {
            c.name("col1");
            c.type(ColumnType.Int8);
          });
        t.column( c -> {
            c.name("col2");
            c.type(ColumnType.Varchar);
          });
      });
    System.out.println(tbl);
  }

}
