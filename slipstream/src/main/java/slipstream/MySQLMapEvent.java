package slipstream;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;
import jetbrains.exodus.env.*;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import org.jetbrains.annotations.NotNull;
import jetbrains.exodus.management.*;
import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.bindings.LongBinding.entryToLong;
import static jetbrains.exodus.bindings.LongBinding.longToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.time.*;

public class MySQLMapEvent {
  private long tableId;
  private TableMapEventData data;

  public MySQLMapEvent(long tableId, TableMapEventData data) {
    this.tableId = tableId;
    this.data = data;
  }

  public ByteIterable getKey() {
    return null;
  }

  public ByteIterable getValue() {
    return null;
  }

  public static ByteIterable get(String key) {
    return stringToEntry(key);
  }

  public static ByteIterable get(String key, long ts) {
    ByteIterable[] segs = new ByteIterable[2];
    segs[0] = stringToEntry(key);
    segs[1] = longToEntry(ts);
    return new CompoundByteIterable(segs);
  }

  public static String getKey(ByteIterable key) {
    byte[] bytes = key.getBytesUnsafe();
    return entryToString(key);
  }

  public static long getTS(ByteIterable key) {
    byte[] bytes = key.getBytesUnsafe();
    byte[] d = new byte[8];
    System.arraycopy(bytes, key.getLength()-8, d, 0, 8);
    return entryToLong(new ArrayByteIterable(d));
  }

  private static byte[] toBytes(Object o) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(o);
      return bos.toByteArray();
    } catch (IOException e) {
    }
    return null;
  }

  private static Object toObject(byte[] bytes) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInput in = new ObjectInputStream(bis)) {
      return in.readObject();
    } catch (IOException e) {
    } catch (ClassNotFoundException e) {
    }
    return null;
  }

}
