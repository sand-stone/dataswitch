package slipstream;

import jetbrains.exodus.env.*;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ArrayByteIterable;
import org.jetbrains.annotations.NotNull;
import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.ByteArrayEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ByteArrayEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import io.atomix.catalyst.buffer.*;
import io.atomix.catalyst.serializer.*;

public class Acme {
  private static Logger log = LogManager.getLogger(Acme.class);

  public static void test1(String[] args) {
    final Environment env = Environments.newInstance("data");
    log.info("create env");
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("MyStore", WITHOUT_DUPLICATES, txn);
        }
      });

    @NotNull final ByteIterable key = stringToEntry("myKey");
    @NotNull final ByteIterable value = stringToEntry("myValue");

    log.info("put value");
    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          store.put(txn, key, value);
        }
      });

    log.info("get value");
    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          final ByteIterable entry = store.get(txn, key);
          assert entry == value;
          System.out.println(entryToString(entry));
        }
      });
    env.close();
  }

  public static void test2(String[] args) {
    final Environment env = Environments.newInstance("data");
    log.info("create env");
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("MyStore", WITHOUT_DUPLICATES, txn);
        }
      });

    log.info("put value");
    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          for (int i=0;i<1000;i++) {
            @NotNull ByteIterable key = stringToEntry("myKey"+i);
            @NotNull ByteIterable value = stringToEntry("myValue"+i);
            store.put(txn, key, value);
          }
        }
      });

    log.info("get value");
    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          for (int i=0;i<1000;i+=10) {
            ByteIterable key = stringToEntry("myKey"+i);
            ByteIterable entry = store.get(txn, key);
            log.info(entryToString(entry));
          }
        }
      });

    env.close();
  }

  public static void test3(String[] args) {
    final Environment env = Environments.newInstance("data");
    log.info("create env");
    final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("MyStore", WITHOUT_DUPLICATES, txn);
        }
      });

    log.info("put value");
    @NotNull final ByteIterable key = stringToEntry("myKey");
    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          for (int i=0;i<10000;i++) {
            @NotNull ByteIterable value = stringToEntry("myValue"+i);
            store.put(txn, key, value);
          }
        }
      });

    log.info("get value");
    env.executeInTransaction(new TransactionalExecutable() {
        @Override
        public void execute(@NotNull final Transaction txn) {
          for (int i=0;i<1000;i+=10) {
            ByteIterable entry = store.get(txn, key);
            log.info(entryToString(entry));
          }
        }
      });

    env.close();
  }

  public static void test4(String[] args) {
    try {
      Serializer serializer = new Serializer(new UnpooledHeapAllocator());
      serializer.register(EventHeader.class, 1);
      serializer.register(EventData.class, 2);
      EventDeserializer eventDeserializer = new EventDeserializer();
      eventDeserializer.setEventDataDeserializer(EventType.XID, new ByteArrayEventDataDeserializer());
      eventDeserializer.setEventDataDeserializer(EventType.QUERY, new ByteArrayEventDataDeserializer());
      BinaryLogFileReader reader = new BinaryLogFileReader(new FileInputStream(args[0]), eventDeserializer);
      for (Event event; (event = reader.readEvent()) != null; ) {
        Object h = event.getHeader();
        Object d = event.getData();
        if(d==null) continue;
        Buffer buffer = serializer.writeObject(h);
        buffer.flip();
        h = serializer.readObject(buffer);
        Buffer buffer2 = serializer.writeObject(d);
        buffer2.flip();
        d = serializer.readObject(buffer2);
        EventType eventType = ((EventHeader)h).getEventType();
        if (eventType == EventType.XID) {
          log.info(d.toString());
        } else
          log.info(eventType);
        log.info(d);
      }
      reader.close();
    } catch(Exception e) {}
    finally {
    }
  }

  public static ArrayByteIterable toArrayByte(Buffer buf) {
    byte[] data = new byte[(int)buf.remaining()];
    buf.read(data);
    return new ArrayByteIterable(data, data.length);
  }

  private static void dump(Buffer buf) {
    byte[] data = new byte[(int)buf.remaining()];
    buf.read(data);
    for (int i=0;i<10;i++) {
      System.out.print(data[i]);System.out.print(' ');
    }
    System.out.println();
  }

  private static void compare(ArrayByteIterable buf1, Buffer buf2) {
    byte[] data1 = buf1.getBytesUnsafe();
    buf2.flip();
    byte[] data2 = new byte[(int)buf2.remaining()];
    buf2.read(data2);
    if(data1.length!=data2.length) {
      System.out.println("******length not equal"); return;
    }
    for (int i=0;i<data1.length;i++) {
      if(data1[i]!=data2[i]) {
        System.out.println("******data not equal"); return;
      }
    }
    System.out.println("xxxx buf the same xxxx");
  }

  private static void dumpinfo(Buffer buf) {
    System.out.println("buf:" + buf.limit() + " remaining:"+buf.remaining());
  }

  private static void dumpinfo(ArrayByteIterable buf) {
    byte[] data = buf.getBytesUnsafe();
    System.out.println("buf:" + data.length);
  }

  public static void test5(final String[] args) {
    try {
      log.info("create events database");
      final Environment env = Environments.newInstance("data");
      final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
          @Override
          public Store compute(@NotNull final Transaction txn) {
            return env.openStore("MyStore", WITHOUT_DUPLICATES, txn);
          }
        });
      log.info("parse and publish events");
      env.executeInTransaction(new TransactionalExecutable() {
          @Override
          public void execute(@NotNull final Transaction txn) {
            try {
            Serializer serializer = new Serializer(new UnpooledHeapAllocator());
            serializer.register(EventHeader.class, 1);
            serializer.register(EventData.class, 2);
            EventDeserializer eventDeserializer = new EventDeserializer();
            eventDeserializer.setEventDataDeserializer(EventType.XID, new ByteArrayEventDataDeserializer());
            eventDeserializer.setEventDataDeserializer(EventType.QUERY, new ByteArrayEventDataDeserializer());
            BinaryLogFileReader reader = new BinaryLogFileReader(new FileInputStream(args[0]), eventDeserializer);
            for (Event event; (event = reader.readEvent()) != null; ) {
              Object h = event.getHeader();
              Object d = event.getData();
              if(d==null) continue;
              Buffer buffer = serializer.writeObject(h);
              buffer.flip();
              ByteIterable key = toArrayByte(buffer);
              //compare((ArrayByteIterable)key, buffer);
              buffer = serializer.writeObject(d);
              buffer.flip();
              ByteIterable value = toArrayByte(buffer);
              store.put(txn, key, value);
            }
            reader.close();
            } catch (Exception e) {e.printStackTrace();}
          }
        });

      log.info("subscribe events");
      env.executeInTransaction(new TransactionalExecutable() {
          @Override
          public void execute(@NotNull final Transaction txn) {
            Serializer serializer = new Serializer(new UnpooledHeapAllocator());
            serializer.register(EventHeader.class, 1);
            serializer.register(EventData.class, 2);
            try (Cursor cursor = store.openCursor(txn)) {
              while (cursor.getNext()) {
                ByteIterable key = cursor.getKey();
                ByteIterable value = cursor.getValue();
                EventHeader h = (EventHeader)serializer.readObject(HeapBuffer.wrap(key.getBytesUnsafe()));
                EventData d = (EventData)serializer.readObject(HeapBuffer.wrap(value.getBytesUnsafe()));
                log.info("key {} value {}", h, d);
              }
            }
          }
        });
      env.close();
    } catch(Exception e) {
      log.info(e);
      System.out.println(e);
    }
    finally {
    }
  }

  public static void main(String[] args) {
    test5(args);
  }

}
