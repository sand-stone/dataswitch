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

public class MySQLTransactionEvent {
  long serverid;
  String table;
  long timestamp;
  EventType eventType;
  TableMapEventData mapEvent;
  EventData cudEvent;

  public MySQLTransactionEvent(long serverid, String table, long timestamp,
                               EventType eventType, TableMapEventData mapEvent, EventData cudEvent) {
    this.serverid = serverid;
    this.table = table;    
    this.timestamp = timestamp;
    this.eventType = eventType;
    this.mapEvent = mapEvent;
    this.cudEvent = cudEvent;
  }

  public ByteIterable getKey() {
    ByteIterable[] segs = new ByteIterable[3];
    segs[0] = longToEntry(serverid);
    segs[1] = stringToEntry(table);
    segs[2] = longToEntry(timestamp);
    return new CompoundByteIterable(segs);
  }

  public ByteIterable getValue() {
    ByteIterable[] segs = new ByteIterable[3];
    segs[0] = new ArrayByteIterable(toBytes(eventType));
    segs[1] = new ArrayByteIterable(toBytes(mapEvent));
    segs[2] = new ArrayByteIterable(toBytes(cudEvent));
    return new CompoundByteIterable(segs);
  }

  public static long getSeverId(ByteIterable key) {
    return entryToLong(key);
  }

  public static String getTable(ByteIterable key) {
    byte[] bytes = key.getBytesUnsafe();
    byte[] d = new byte[key.getLength()-8];
    System.arraycopy(bytes, 8, d, 0, d.length);    
    return entryToString(new ArrayByteIterable(d));
  }
  
  public static long getTimestamp(ByteIterable key) {
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
