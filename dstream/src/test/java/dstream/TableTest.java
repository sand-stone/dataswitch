package dstream;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import static dstream.Table.*;
import static dstream.Table.TableBuilder.*;
import static dstream.Table.RowBuilder.*;

public class TableTest extends TestCase
{
  public TableTest(String testName) {
    super(testName);
  }

  public static Test suite() {
    return new TestSuite(TableTest.class);
  }

  public void testCreateTable() {
    Table tbl = Table("acme", t -> {
        t.column( c -> {
            c.name("col1");
            c.type(ColumnType.Int8);
          });
        t.column( c -> {
            c.name("col2");
            c.type(ColumnType.Varchar);
          });
      });
    assertTrue(true);
  }

  public void testInsertRow() {
    Table tbl = Table("acme", (t -> {
          t.column(c -> {
              c.name("col1");
              c.type(ColumnType.Int8);
            });
          t.column( c -> {
              c.name("col2");
              c.type(ColumnType.Varchar);
            });
        }));

    Row row = Row(r -> {
        r.field(f -> {
            f.field("col1", 12345);
          });
        r.field(f -> {
            f.field("col2", "acme");
          });
      });
    tbl.insert(row);
    assertTrue(true);
  }

}
