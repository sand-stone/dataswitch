package dstream;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import static dstream.Table.*;
import static dstream.Table.TableBuilder.*;

/**
 * Unit test for simple App.
 */
public class TableTest extends TestCase
{
  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public TableTest(String testName)
  {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite()
  {
    return new TestSuite(TableTest.class);
  }

  /**
   * Rigourous Test :-)
   */
  public void testApp()
  {
    Table tbl = Table(t -> {
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

    assertTrue( true );
  }

}

