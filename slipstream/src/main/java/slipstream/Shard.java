package slipstream;

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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;

public class Shard implements Runnable {
  private static Logger log = LogManager.getLogger(Shard.class);

  private LinkedBlockingQueue<MySQLTransactionEvent> evtsq;
  Environment env;
  Store store;
  int id;

  public Shard(String dir, int id) {
    this.id = id;
    env = Environments.newInstance(dir);
    store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore("slipstream#"+dir, WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
        }
      });
    this.evtsq = new LinkedBlockingQueue<MySQLTransactionEvent>();
    log.info("create shard {}", dir);
  }


  public void run() {

  }

  public Queue getEventQueue() {
    return evtsq;
  }

  public Environment getEnv() {
    return env;
  }

  public Store getStore() {
    return store;
  }

  class WriteTask implements Runnable {
    public WriteTask() {
    }

    public void run() {
      final int[] count = new int[1];
      log.info("write task starts");
      while(true) {
        count[0] = 0;
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
              try {
                int batch=10000;
                long t1 = System.nanoTime();
                while (evtsq.size() > 0) {
                  if (count[0] >= batch)
                    break;
                  MySQLTransactionEvent evt = evtsq.poll();
                  if(evt == null) break;
                  store.put(txn, evt.getKey(), evt.getValue());
                  count[0]++;
                }
                long t2 = System.nanoTime();
                log.info("commit {} transactions in {} mill-seconds", count[0], (t2-t1)/1e6);
              } catch (Exception e) {
                log.info(e);
              }
            }
          });
      }
    }
  }

}
