package slipstream;

import jetbrains.exodus.env.*;
import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;
import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

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
  
  public static void main(String[] args) {
    test3(args);
  }
}
