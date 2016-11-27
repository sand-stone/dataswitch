package slipstream;

import kdb.Client;
import java.util.*;
import kdb.KdbException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KdbConnector {
  private static Logger log = LogManager.getLogger(KdbConnector.class);
  private List<String> uris;
  private Client client;
  private ConcurrentLinkedQueue<MySQLChangeRecord> queue;
  private int batchSize = 100;

  public static KdbConnector get() {
    return new KdbConnector(Arrays.asList("http://localhost:8000"), "mysqlevents");
  }

  public KdbConnector(List<String> uris, String table) {
    this.queue = new ConcurrentLinkedQueue<MySQLChangeRecord>();
    this.uris = new ArrayList(uris);
    client = new Client(uris.get(0), table);
    client.open();
  }

  public void publish(MySQLChangeRecord evt) {
    queue.add(evt);
    if(queue.size() >= batchSize) {
      send();
    }
  }

  private synchronized void send() {
    if(queue.size() < batchSize)
      return;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    queue.forEach(evt -> {
        keys.add(evt.key());
        values.add(evt.value());
      });
    write(keys, values);
    queue.clear();
  }

  private void write(List<byte[]> keys, List<byte[]> values) {
    try {
      client.put(keys, values);
      log.info("send {} records", keys.size());
    } catch(KdbException e) {
      log.info(e);
    }
  }

  public int count() {
    int count = 0;
    Client.Result rsp = client.scanFirst(2);
    count += rsp.count();
    while(rsp.token().length() > 0) {
      try {
        rsp = client.scanNext(2);
      } catch(KdbException e) {
        log.info(e);
      }
      count += rsp.count();
    }
    return count;
  }

}
