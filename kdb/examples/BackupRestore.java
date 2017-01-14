import kdb.Client;
import kdb.KdbException;
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

  private static void addData(Client client) {
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
      client.put(wkeys, values);
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
      client.put(wkeys, values);
    }
  }

  static byte[] valueState;

  public static long getLastLSN(String uri, String table) {
    try (Client client = new Client(uri, table)) {
      client.open();
      return client.getLatestSequenceNumber();
    } catch(Exception e) {}
    return -1;
  }

  public static void main(String[] args) {

    try (Client client = new Client("http://localhost:8000/", "acme")) {
      client.open();
      addData(client);
      log.info("backups {}", client.listBackup());
      client.backup();
      try { Thread.currentThread().sleep(3000);} catch(Exception e) {}
      log.info("backups {}", client.listBackup());
      client.backup();
      try { Thread.currentThread().sleep(3000);} catch(Exception e) {}
      log.info("backups {}", client.listBackup());
      log.info("restore: {}", client.restore(-1));
    }

    log.info("lsn {}", getLastLSN("http://localhost:8000/", "acme"));
    System.exit(0);
  }
}
