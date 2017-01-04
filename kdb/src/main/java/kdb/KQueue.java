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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class KQueue implements Closeable {
  private static Logger log = LogManager.getLogger(KQueue.class);
  private static KQueue instance = new KQueue();
  LinkedBlockingQueue<SequenceOperation> queue;
  static final int MAX_PENDING_REQS = 10000;
  Thread[] workers;

  private KQueue() {
    queue = new LinkedBlockingQueue<>(MAX_PENDING_REQS);
    workers = new Thread[Runtime.getRuntime().availableProcessors()*2];
    for(int i = 0; i < workers.length; i++) {
      workers[i] = new Thread(new Updater());
      workers[i].start();
    }
  }

  public void close() {}

  public static KQueue get() {
    return instance;
  }

  public void add(SequenceOperation op) {
    try {
      queue.put(op);
    } catch(InterruptedException e) {
      log.info("missed {}", op);
    }
  }

  public int size() {
    return queue.size();
  }

  private class Updater implements Runnable {
    Client client;

    public Updater() {
      client = new Client();
    }

    public void run() {
      while(true) {
        try {
          SequenceOperation op = queue.take();
          long seqno = op.getSeqno();
          Client.Result rsp = client.scanlog("http://"+op.getEndpoint(), op.getTable(), seqno, 1);
          Store.get().update(op.getTable(), rsp);
        } catch(Exception e) {
          e.printStackTrace();
          //log.info("failed to reach master {} exception: {}", op.getEndpoint(), e);
        }
      }
    }
  }

}
