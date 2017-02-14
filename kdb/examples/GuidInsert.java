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
import com.google.gson.Gson;

public class GuidInsert {

  private static void process(Client client, List<byte[]> eventKeys) {
    int batchSize = 1000;
    List<byte[]> values = new ArrayList<byte[]>();
    List<byte[]> keys;
    int index = 0; int step = 100;
    int pcount = 0;
    int existedKeys  = 0;
    Random rnd = new Random();

    for(index = 0; (index + step) < eventKeys.size(); index += step) {
      keys = eventKeys.subList(index, index+step);
      Client.Result rsp = client.get(keys);
      existedKeys += rsp.count();
      for(int i = 0; i < keys.size(); i++) {
        values.add(Arrays.copyOf(valueState, valueState.length));
      }
      long s1 = System.nanoTime();
      client.put(keys, values);
      long s2 = System.nanoTime();
      pcount += keys.size();
      values.clear();
    }

    if(index < eventKeys.size()) {
      values.clear();
      keys = eventKeys.subList(index, eventKeys.size());
      for(int i = 0; i < keys.size(); i++) {
        byte[] value = new byte[1000*8];
        rnd.nextBytes(value);
        values.add(value);
      }
      pcount += keys.size();
      client.put(keys, values);
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

  public static class Options {
    String CompactionStyle;
    //long MaxTableFilesSizeFIFO;
    int MaxBackgroundFlushes;
    int MaxBackgroundCompactions;
    int MaxWriteBufferNumber;
    int MinWriteBufferNumberToMerge;
    int WalTtlSeconds;
    long WalSizeLimitMB;
    int NumLevel;
    long MaxBytesForLevelBase;
    int MaxBytesForLevelMultiplier;
  }

  private static String evtopts() {
    Options options = new Options();
    options.CompactionStyle = "UNIVERSAL";
    //options.MaxTableFilesSizeFIFO = 1024*1024*1024*3L;
    options.MaxBackgroundFlushes = 8;
    options.MaxBackgroundCompactions = 8;
    options.MaxWriteBufferNumber = 8;
    options.MinWriteBufferNumberToMerge = 8;
    options.NumLevel = 10;
    options.MaxBytesForLevelBase = 32*1024*1024;
    options.MaxBytesForLevelMultiplier = 4;

    options.WalTtlSeconds = 60*8;
    options.WalSizeLimitMB = 15000;
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  public static void main(String[] args) {
    Random rnd = new Random();

    int count = 1000000;
    int batch = 100000;

    try (Client client = new Client("http://localhost:8000/", "acme", evtopts())) {
      client.open();
      for(int k = 0; k < count; k++) {
        List<byte[]> keys = new ArrayList<byte[]>();
        List<byte[]> values = new ArrayList<byte[]>();
        for(int i = 0; i < batch; i++) {
          byte[] key = new byte[50];
          rnd.nextBytes(key);
          byte[] value = new byte[4];
          rnd.nextBytes(value);
          keys.add(key);
          values.add(value);
        }
        long t1 = System.nanoTime();
        client.put(keys, values);
        long t2 = System.nanoTime();
        System.out.println("insert time: " +  (t2-t1)/1e9);
      }
    }
    System.exit(0);
  }
}
