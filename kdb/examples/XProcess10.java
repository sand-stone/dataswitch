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

public class XProcess10 {

  private static String events = "xevents";
  private static String states = "xstates";

  private static String[] uris;

  private static UUID[] deviceIds;

  private static int range = 6;

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
        try (Client client = new Client(uris[0], events+bucket)) {
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

  public static class Update implements Callable<Integer> {
    private int id;
    List<byte[]> eventKeys;
    private byte[] valueState;
    private Random rnd;

    public Update(int id, List<byte[]> eventKeys) {
      this.id = id;
      this.eventKeys = eventKeys;
      rnd = new Random();
      valueState = new byte[7000*8]; //7000
      rnd.nextBytes(valueState);
    }

    public Integer call() {
      try (Client client = new Client(uris[0], states)) {
        long s1 = System.nanoTime();
        client.open();
        List<byte[]> values = new ArrayList<byte[]>();
        List<byte[]> keys = new ArrayList<byte[]>();
        int index = 0;
        int count = 0;
        int existedKeys  = 0;
        //for(index = 0; index < eventKeys.size(); index+= rnd.nextInt(100)) {
        for(index = 0; index < eventKeys.size(); index++) {
          keys.add(eventKeys.get(index));
          values.add(Arrays.copyOf(valueState, valueState.length));
          if(keys.size() > 1000) {
            Client.Result rsp = client.get(keys);
            existedKeys += rsp.count();
            client.put(keys, values);
            count += keys.size();
            keys.clear();
            values.clear();
          }
        }
        if(keys.size() > 0) {
          Client.Result rsp = client.get(keys);
          existedKeys += rsp.count();
          client.put(keys, values);
          count += keys.size();
        }
        long s2 = System.nanoTime();
        System.out.printf("update %d state writing keys %d/%d in %e seconds \n", id, count, existedKeys, (s2-s1)/1e9);
      }
      return 0;
    }
  }

  public static class Query implements Runnable {
    private int id;
    private byte[] valueState;
    private Random rnd;
    private ForkJoinPool workerPool;

    public Query(int id) {
      this.id = id;
      workerPool = new ForkJoinPool(2);
      rnd  = new Random();
    }

    private int queryEvents(byte bucket) {
      int uid = 0;
      int count = 0;
      int total = 0;
      try (Client eventClient = new Client(uris[0], events+bucket)) {
        eventClient.open();
        List<byte[]> keys = new ArrayList<byte[]>();
        Client.Result rsp = eventClient.scanFirst(1000);
        count += rsp.count();
        total += rsp.count();
        keys.addAll(rsp.keys());
        while(rsp.token().length() > 0) {
          try {
            rsp = eventClient.scanNext(1000);
            //keys.addAll(rsp.keys());
            count += rsp.count();
            total += rsp.count();
            if(count > 200000) {
              //workerPool.submit(new Update(uid++, keys));
              //keys = new ArrayList<byte[]>();
              count = 0;
            }
          } catch(KdbException e) {
            System.out.println(e);
          }
        }
        if(count > 0) {
          //workerPool.submit(new Update(uid++, keys));
        }
      }
      return total;
    }

    public void run() {
      while(true) {
        int i = rnd.nextInt(range);
        byte bucket = (byte)i;
        try {
          long t1 = System.nanoTime();
          int total = queryEvents(bucket);
          long t2 = System.nanoTime();
          if(total > 0)
            System.out.printf("read %d for bucket %d at total %d %e rate %e \n", id, bucket, total, (t2-t1)/1e9, (1.0*total)/((t2-t1)/1e9));
          while (workerPool.getQueuedTaskCount() > 30) {
            System.out.printf("update tasks %d \n", workerPool.getQueuedTaskCount());
            try {Thread.currentThread().sleep(1000);} catch(InterruptedException ex) {}
          }
        } catch(Exception e) {
          System.out.printf(" processor %d get %s \n", id, e.getMessage());
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
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  private static String statesopts() {
    Options options = new Options();
    options.CompactionStyle = "FIFO";
    options.MaxTableFilesSizeFIFO = 1024*1024*1024*60L;
    options.MaxBackgroundFlushes = 2;
    options.MaxBackgroundCompactions = 4;
    options.MaxWriteBufferNumber = 32;
    options.MinWriteBufferNumberToMerge = 8;
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  private static void init() {
    deviceIds = new UUID[1000000]; //(150000000/3600.0)*300/100
    for(int i = 0; i < deviceIds.length; i++) {
      deviceIds[i] = UUID.randomUUID();
    }
  }

  public static Client[] getClients(String uri) {
    Client[] clients = new Client[range];
    for(int i = 0; i < range; i++) {
      clients[i] = new Client(uri, events+i);
      clients[i].open();
    }
    return clients;
  }

  public static void main(String[] args) {
    uris = new String[]{"http://10.0.0.10:8000/", "http://10.0.0.11:8000/", "http://10.0.0.12:8000/"};

    String uri = uris[0];
    System.out.println("start");
    init();
    System.out.println("create events table");
    for(int i = 0; i < range; i++) {
      try (Client client = new Client(uri, events+i, evtopts())) {
        client.open("append");
      }
    }

    System.out.println("create states table");
    try (Client client = new Client(uri, states, statesopts())) {
      //client.open("append", 30*60);
      client.openCompressed("snappy");
    }

    System.out.println("start event source threads");
    int num = 3;
    for (int i = 0; i < num; i++) {
      new Thread(new EventSource(i)).start();
    }

    /*
    System.out.println("start query worker");
    for (int i = 0; i < 5; i++) {
      new Thread(new Query(i)).start();
      }*/

    Client[] mclients = getClients("http://10.0.0.10:8000/");
    Client[] s1clients = getClients("http://10.0.0.11:8000/");
    Client[] s2clients = getClients("http://10.0.0.12:8000/");

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
