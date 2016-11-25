package slipstream;

import kdb.Client;
import java.util.*;
import kdb.KdbException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class KdbConnector {
  private static Logger log = LogManager.getLogger(KdbConnector.class);
  private List<String> uris;
  private Client client;

  public KdbConnector(List<String> uris, String table) {
    this.uris = new ArrayList(uris);
    client = new Client(uris.get(0), table);
    client.open();
  }

  public void write(List<byte[]> keys, List<byte[]> values) {
    try {
      client.put(keys, values);
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
