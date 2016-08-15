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

public class Shard {
  private static Logger log = LogManager.getLogger(Shard.class);
  Environment env;
  Store store;

  public Shard(String dir) {
    env = Environments.newInstance(dir);
    store = env.computeInTransaction(new TransactionalComputable<Store>() {
        @Override
        public Store compute(@NotNull final Transaction txn) {
          return env.openStore(dir, WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
        }
      });
  }

  public void write(Object msg) {
    log.info("msg {} = {}",msg.getClass(), msg);
    if(msg instanceof MySQLTransactionEvent) {
      MySQLTransactionEvent evt = (MySQLTransactionEvent)msg;
      env.executeInTransaction(new TransactionalExecutable() {
          @Override
          public void execute(@NotNull final Transaction txn) {
            try {
              store.put(txn, evt.getKey(), evt.getValue());
            } catch(IOException e) {
              log.info(e);
              txn.abort();
            }
          }
        });
    }
  }

}
