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
import java.time.LocalDateTime;
import com.google.gson.Gson;

public class XProcess11 {

  private static String[] uris;

  private static UUID[] deviceIds;

  private static int range = 6;
  private static String[] evtTables;

  public static class EventSource implements Runnable {
    private int id;
    private Random rnd;
    private int valSize;

    public EventSource(int id) {
      this.id  = id;
      rnd = new Random();
      valSize = 300;
    }

    private void write(Client client, List<byte[]> keys, List<byte[]> values) {
      int retry = 5;
      do {
        try {
          client.put(keys, values);
          return;
        } catch(KdbException e) {
          retry--;
        }
      } while(retry>0);
      throw new KdbException("timed out");
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int batchSize = 1000;
      long total = 0;
      int numids = 125000;
      while(true) {
        LocalDateTime now = LocalDateTime.now();
        int hour = now.getHour();
        int m = now.getMinute();
        int bucket = m == 60? 0: m/10;
        try (Client client = new Client(uris[0], evtTables[bucket])) {
          client.open();
          keys.clear();
          values.clear();
          long t1 = System.nanoTime();
          int count = 0;
          for(int i = 0; i < numids; i++) {
            UUID guid = deviceIds[rnd.nextInt(deviceIds.length)];
            for(int k = 0; k < 6; k++) {
              ByteBuffer key = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
              key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
              keys.add(key.array());
              byte[] value = new byte[valSize];
              rnd.nextBytes(value);
              values.add(value);
            }
            if(keys.size() >= batchSize) {
              write(client, keys, values);
              total += keys.size();
              count += keys.size();
              keys.clear();
              values.clear();
            }
          }
          if(keys.size() > 0) {
            write(client, keys, values);
            total += keys.size();
            count += keys.size();
          }
          long t2 = System.nanoTime();
          System.out.printf("write %d total %d for bucket %d at count %d time %e rate of %e \n", id, total, bucket, count, (t2-t1)/1e9,
                            (1.0*count)/((t2-t1)/1e9));
        } catch(KdbException e) {
          System.out.printf("event source %d failed", id);
        }
      }
    }
  }

  public static class Options {
    String CompactionStyle;
    long MaxTableFilesSizeFIFO;
    int MaxBackgroundFlushes;
    int MaxBackgroundCompactions;
    int MaxWriteBufferNumber;
    int MinWriteBufferNumberToMerge;
    int WalTtlSeconds;
    long WalSizeLimitMB;
  }

  private static String evtopts() {
    Options options = new Options();
    options.CompactionStyle = "FIFO";
    options.MaxTableFilesSizeFIFO = 1024*1024*1024*3L;
    options.MaxBackgroundFlushes = 2;
    options.MaxBackgroundCompactions = 8;
    options.MaxWriteBufferNumber = 16;
    options.MinWriteBufferNumberToMerge = 8;
    options.WalTtlSeconds = 60*5;
    options.WalSizeLimitMB = 10000;
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  private static void init() {
    deviceIds = new UUID[1000000]; //(150000000/3600.0)*300/100
    for(int i = 0; i < deviceIds.length; i++) {
      deviceIds[i] = UUID.randomUUID();
    }

    Random rnd = new Random();
    evtTables = new String[range];
    for (int i = 0; i < range; i++) {
      evtTables[i] = "evt"+ Math.abs(rnd.nextInt());
    }
  }

  public static Client[] getClients(String uri) {
    Client[] clients = new Client[range];
    for(int i = 0; i < range; i++) {
      clients[i] = new Client(uri, evtTables[i]);
      clients[i].open();
    }
    return clients;
  }

  public static void main(String[] args) {
    uris = new String[]{"http://10.0.0.10:8000/", "http://10.0.0.11:8000/", "http://10.0.0.12:8000/"};
    //uris = new String[]{"http://127.0.0.1:8000/", "http://127.0.0.1:8002/", "http://127.0.0.1:8004/"};

    String uri = uris[0];
    System.out.println("start");
    init();
    System.out.println("create events table");
    for(int i = 0; i < range; i++) {
      try (Client client = new Client(uri, evtTables[i], evtopts())) {
        client.open("append");
      }
    }

    System.out.println("start event source threads");
    int num = 1;
    for (int i = 0; i < num; i++) {
      new Thread(new EventSource(i)).start();
    }

    Client[] mclients = getClients("http://10.0.0.10:8000/");
    //Client[] mclients = getClients("http://localhost:8000/");
    Client[] s1clients = getClients("http://10.0.0.11:8000/");
    //Client[] s1clients = getClients("http://localhost:8002/");
    Client[] s2clients = getClients("http://10.0.0.12:8000/");
    //Client[] s2clients = getClients("http://localhost:8004/");

    while(true) {
      try { Thread.currentThread().sleep(3000); } catch(Exception e) {}
      for(int i = 0; i < range; i++) {
        System.out.printf("master xevents%d LSN %d \n", i, mclients[i].getLatestSequenceNumber());
        System.out.printf("slave1 xevents%d LSN %d \n", i, s1clients[i].getLatestSequenceNumber());
        System.out.printf("slave2 xevents%d LSN %d \n", i, s2clients[i].getLatestSequenceNumber());
      }
    }
  }

}
