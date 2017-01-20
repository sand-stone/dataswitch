import org.rocksdb.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalTime;

public class LogTest2 {

  static RocksDB createDB(String name, String wal) {
    Options options = new Options().setCreateIfMissing(true);
    options.setWalSizeLimitMB(1000);
    options.setWalTtlSeconds(100);
    if(wal != null)
      options.setWalDir(wal);
    RocksDB db = null;
    try {
      db = RocksDB.open(options, name);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
    return db;
  }

  static int memcmp(final byte[] a, final byte[] b, int len) {
    for (int i = 0; i < len; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);
      }
    }
    return 0;
  }

  static RocksIterator rangeCursor(RocksDB db, byte[] prefix) {
    ReadOptions readOptions = new ReadOptions();
    readOptions.setTotalOrderSeek(true);
    readOptions.setPrefixSameAsStart(true);
    RocksIterator cursor = db.newIterator(readOptions);
    cursor.seek(prefix);
    return cursor;
  }

  static void scan(RocksIterator cursor, byte[] suffix, List<byte[]> keys, List<byte[]> values, int limit) {
    int count = 0;
    while(cursor.isValid()) {
      byte[] key = cursor.key();
      byte[] value = cursor.value();
      if(memcmp(key, suffix, suffix.length) < 0) {
        keys.add(Arrays.copyOf(key, key.length));
        values.add(Arrays.copyOf(value, value.length));
      } else {
        break;
      }
      cursor.next();
      if(count++ > limit)
        return;
    }
  }

  static void put(RocksDB db, List<byte[]> keys, List<byte[]> values) {
    try(WriteOptions writeOpts = new WriteOptions();
        WriteBatch writeBatch = new WriteBatch()) {
      for(int i = 0; i < keys.size(); i++) {
        writeBatch.put(keys.get(i), values.get(i));
      }
      db.write(writeOpts, writeBatch);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void gendata(int count, List<byte[]> keys, List<byte[]> values) {
    for(int i = 0; i < count; i++) {
      byte[] data = new byte[20];
      rnd.nextBytes(data);
      keys.add(data);
      data = new byte[300];
      rnd.nextBytes(data);
      values.add(data);
    }
  }

  static Random rnd = new Random();

  public static void main(String[] args) throws Exception {
    RocksDB.loadLibrary();

    long t1 = System.nanoTime();
    RocksDB acme = createDB("acme", args.length == 0? null : args[0]);
    int batch = 1000;
    int count = 10000;
    for (int i = 0; i < count; i++) {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      gendata(batch, keys, values);
      put(acme, keys, values);
    }

    acme.flush(new FlushOptions());
    long t2 = System.nanoTime();

    System.out.printf("total %e\n", (t2-t1)/1e9);
  }

}
