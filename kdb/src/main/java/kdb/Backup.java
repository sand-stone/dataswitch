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
import com.google.gson.Gson;
import kdb.proto.Database.*;
import kdb.proto.Database.Message.MessageType;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class Backup implements AutoCloseable {
  private static Logger log = LogManager.getLogger(Backup.class);
  BackupEngine engine;
  RocksDB db;
  LinkedBlockingQueue<BackupOperation> queue;
  static final int MAX_PENDING_REQS = 10;
  Thread worker;
  Gson gson;
  String path;

  public Backup(String tablename, String path, RocksDB db) {
    try {
      String backpath = Config.get().getString("store.backup");
      if(backpath == null || backpath.isEmpty()) {
        backpath = path + "/backups";
      } else {
        backpath += "/" + tablename;
      }
      Utils.mkdir(backpath);
      engine = BackupEngine.open(Env.getDefault(), new BackupableDBOptions(backpath));
      queue = new LinkedBlockingQueue<>(MAX_PENDING_REQS);
      worker = new Thread(new BackupTask());
      this.db = db;
      this.path = path;
      gson = new Gson();
    } catch(RocksDBException e) {
      throw new KdbException(e);
    }
  }

  public Message list() {
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    int count = 0;
    for(BackupInfo info: engine.getBackupInfo()) {
      String val = gson.toJson(info);
      keys.add((""+count++).getBytes());
      values.add(val.getBytes());
    }
    return  MessageBuilder.buildResponse("", keys, values);
  }

  public void add(BackupOperation op) {
    //log.info("add backup {}", op);
    try {
      if(!worker.isAlive()) {
        worker.start();
      }
      queue.put(op);

    } catch(InterruptedException e) {
      log.info("missed {}", op);
    }
  }

  private class BackupTask implements Runnable {
    public void run() {
      while(true) {
        try {
          BackupOperation op = queue.take();
          engine.createNewBackup(db, false);
        } catch(InterruptedException e) {
          return;
        } catch(Exception e) {
          log.info("backup failed {}", e.getMessage());
        }
      }
    }
  }

  public Message restore(RestoreOperation op) {
    int id = op.getBackupId();
    Message ret = MessageBuilder.emptyMsg;
    RestoreOptions opt = new RestoreOptions(true);
    try {
      if(id == -1) {
        engine.restoreDbFromLatestBackup(path, path, opt);
      } else {
        engine.restoreDbFromBackup(id, path, path, opt);
      }
    } catch(RocksDBException e) {
      log.info("restore failed {}", e.getMessage());
      ret = MessageBuilder.buildErrorResponse(e.getMessage());
    }
    return ret;
  }

  public void close() {
    worker.interrupt();
    engine.close();
  }
}
