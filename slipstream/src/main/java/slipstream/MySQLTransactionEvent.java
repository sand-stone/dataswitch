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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import org.jetbrains.annotations.NotNull;
import jetbrains.exodus.management.*;
import jetbrains.exodus.bindings.LongBinding;
import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.bindings.LongBinding.entryToLong;
import static jetbrains.exodus.bindings.LongBinding.longToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
import jetbrains.exodus.util.LightOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.time.*;

public class MySQLTransactionEvent {
  long serverid;
  String table;
  String database;
  long timestamp;
  long position;
  TableMapEventData mapEvent;
  EventData cudEvent;

  public static MySQLTransactionEvent get(ByteIterable bytes) {
    final ByteIterator iterator = bytes.iterator();
    long serverid = LongBinding.readCompressed(iterator);
    String database = entryToString(bytes);
    iterator.skip(database.length()+1);
    String table = entryToString(bytes);
    iterator.skip(table.length()+1);
    long timestamp = LongBinding.readCompressed(iterator);
    long position = LongBinding.readCompressed(iterator);
    return new MySQLTransactionEvent(serverid, database, table, timestamp, position);
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

  public ByteIterable getKey() {
    final LightOutputStream output = new LightOutputStream();
    LongBinding.writeCompressed(output, serverid);
    output.writeString(database);
    output.writeString(table);
    LongBinding.writeCompressed(output, timestamp);
    LongBinding.writeCompressed(output, position);
    return output.asArrayByteIterable();
  }

  public ByteIterable getValue() {
    final LightOutputStream output = new LightOutputStream();
    output.write(toBytes(mapEvent));
    output.write(toBytes(cudEvent));
    return output.asArrayByteIterable();
  }

  public MySQLTransactionEvent(long serverid, String database, String table, long timestamp, long position,
                               TableMapEventData mapEvent, EventData cudEvent) {
    this.serverid = serverid;
    this.database = database;
    this.table = table;
    this.timestamp = timestamp;
    this.position = position;
    this.mapEvent = mapEvent;
    this.cudEvent = cudEvent;
  }

  MySQLTransactionEvent(long serverid, String database, String table, long timestamp, long position) {
    this.serverid = serverid;
    this.database = database;
    this.table = table;
    this.timestamp = timestamp;
    this.position = position;
  }

  public String toString() {
    return "<"+ "serverid:" + serverid +
      " database: "+ database +" table:" + table + " timestamp:" + timestamp + " position:"+ position +">";
  }

}
