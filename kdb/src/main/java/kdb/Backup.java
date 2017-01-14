package kdb;

import java.nio.*;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.*;
import static java.util.stream.Collectors.toList;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import org.rocksdb.*;
import kdb.proto.Database.*;
import kdb.proto.Database.Message.MessageType;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class Backup implements AutoCloseable {
  private static Logger log = LogManager.getLogger(Backup.class);
  BackupEngine engine;
  LinkedBlockingQueue<BackupOperation> queue;
  static final int MAX_PENDING_REQS = 10;

  public Backup(String path) {
    try {
      String backpath = path+"/backups";
      Utils.mkdir(backpath);
      engine = BackupEngine.open(Env.getDefault(), new BackupableDBOptions(backpath));
      queue = new LinkedBlockingQueue<>(MAX_PENDING_REQS);
    } catch(RocksDBException e) {
      throw new KdbException(e);
    }
  }

  public void add(BackupOperation op) {
    try {
      queue.put(op);
    } catch(InterruptedException e) {
      log.info("missed {}", op);
    }
  }

  public void restore(RestoreOperation op) {
    log.info("blocking restore {}", op);
  }

  public void close() {
    engine.close();
  }
}
