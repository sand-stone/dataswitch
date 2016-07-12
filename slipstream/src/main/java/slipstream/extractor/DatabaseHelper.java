package slipstream.extractor;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import slipstream.extractor.mysql.SerialBlob;

/**
 * Implements helper methods for database operations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DatabaseHelper
{
  /**
   * Create a serializable blob from a byte array.
   * 
   * @param bytes Array from which to read
   * @throws SQLException Thrown if the safe blob cannot be instantiated.
   */
  public static SerialBlob getSafeBlob(byte[] bytes) throws SQLException
  {
    return getSafeBlob(bytes, 0, bytes.length);
  }

  /**
   * Create a serializable blob from a byte array.
   * 
   * @param bytes Array from which to read
   * @param off Offset into the array
   * @param len Length to read from offset
   * @throws SQLException Thrown if the safe blob cannot be instantiated.
   */
  public static SerialBlob getSafeBlob(byte[] bytes, int off, int len)
    throws SQLException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(bytes, off, len);
    byte[] newBytes = baos.toByteArray();
    return new SerialBlob(newBytes);
  }
}
