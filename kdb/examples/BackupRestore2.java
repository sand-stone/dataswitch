import org.rocksdb.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalTime;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class BackupRestore {
  private static Logger log = LogManager.getLogger(BackupRestore.class);

  private static boolean mkdir(String dir) {
    File d = new File(dir);
    boolean ret = d.exists();
    if(ret && d.isFile())
      throw new RuntimeException("wrong directory:" + dir);
    if(!ret) {
      d.mkdirs();
    }
    return ret;
  }

  private static void addData(RocksDB db) {
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    Random rnd = new Random();
    valueState = new byte[1000*8];
    rnd.nextBytes(valueState);

    for(int i = 0; i < 50000; i++) {
      UUID guid = UUID.randomUUID();
      ByteBuffer key = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
      key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
      keys.add(key.array());
    }

    int batchSize = 1000;
    int index = 0; int step = 100;

    for(index = 0; (index + step) < keys.size(); index += step) {
      List<byte[]> wkeys = keys.subList(index, index+step);
      for(int i = 0; i < wkeys.size(); i++) {
        values.add(Arrays.copyOf(valueState, valueState.length));
      }
      put(db, wkeys, values);
      values.clear();
    }

    if(index < keys.size()) {
      values.clear();
      List<byte[]> wkeys = keys.subList(index, keys.size());
      for(int i = 0; i < wkeys.size(); i++) {
        byte[] value = new byte[1000*8];
        rnd.nextBytes(value);
        values.add(value);
      }
      put(db, wkeys, values);
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

  static byte[] valueState;

  public static long getLastLSN(RocksDB db) {
    return db.getLatestSequenceNumber();
  }

  static RocksDB openDB(String name) throws RocksDBException {
    String path = location+"/"+name;
    mkdir(path);
    try(Options options = new Options().setCreateIfMissing(true)) {
      options.setAllowConcurrentMemtableWrite(true);
      options.setEnableWriteThreadAdaptiveYield(true);
      options.setCompactionStyle(CompactionStyle.UNIVERSAL);
      options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
      options.setWalDir(wal_path);
      try {
        return RocksDB.open(options, path);
      } catch(RocksDBException e) {
        log.info("recover db from {}", path);
      }
      options.setCreateIfMissing(false);
      return RocksDB.open(options, path);
    }
  }


  static String location = "data";
  static String backup_path = location + "/backup/acme";
  static String wal_path = location + "/wal/acme";

  private static void backup(RocksDB db) throws RocksDBException {
    try(BackupableDBOptions opts = new BackupableDBOptions(backup_path)) {
      //opts.setBackupLogFiles(false);
      BackupEngine backup = BackupEngine.open(Env.getDefault(), opts);
      backup.createNewBackup(db, false);
    }
  }

  private static void restore(String name) throws RocksDBException {
    RocksDB db = openDB(name);
    log.info("lsn {} count {}", getLastLSN(db), count(db));
    try(BackupableDBOptions bopts = new BackupableDBOptions(backup_path)) {
      BackupEngine backup = BackupEngine.open(Env.getDefault(), bopts);
      try(RestoreOptions opts = new RestoreOptions(true)) {
        backup.restoreDbFromLatestBackup(location+"/"+name, wal_path, opts);
        //backup.restoreDbFromLatestBackup(location+"/"+name, location+"/"+name, opts);
        log.info("lsn {} count {}", getLastLSN(db), count(db));
      }
      backup.close();
    }
    db.close();
  }

  static private ReadOptions getReadOptions(boolean prefix) {
    ReadOptions readOptions = new ReadOptions();
    readOptions.setTotalOrderSeek(true);
    if(prefix)
      readOptions.setPrefixSameAsStart(prefix);
    return readOptions;
  }

  static private long count(RocksDB db) {
    long count = 0;
    RocksIterator iter = db.newIterator(getReadOptions(false));
    iter.seekToFirst();
    while(iter.isValid()) {
      iter.next();
      count++;
    }
    return count;
  }

  public static void main(String[] args) throws Exception {
    if(args.length == 0) {
      System.out.println("backup or restore");
      return;
    }
    mkdir(backup_path);
    mkdir(wal_path);
    if(args[0].equals("backup")) {
      RocksDB db = openDB("acme");
      log.info("lsn {}", getLastLSN(db));
      addData(db);
      backup(db);
      addData(db);
      backup(db);
      log.info("lsn {} count {}", getLastLSN(db), count(db));
      try { Thread.currentThread().sleep(3000); } catch(Exception e) {}
      db.close();
    } else {
      restore("acme");
    }
  }
}
