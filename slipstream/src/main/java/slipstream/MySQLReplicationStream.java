package slipstream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import slipstream.MySQLChangeRecord.FieldType;
import slipstream.MySQLChangeRecord.OperationType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class MySQLReplicationStream {

  private static Logger log = LogManager.getLogger(MySQLReplicationStream.class);

  private final String hostname;
  private final int port;
  private final String username;
  private final String password;

  private BinaryLogClient binaryLogClient;
  private KdbConnector kdb;

  public MySQLReplicationStream(String username, String password) {
    this("localhost", 3306, username, password);
  }

  public MySQLReplicationStream(String hostname, int port, String username, String password) {
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;
    kdb = KdbConnector.get();
  }

  public void connect() throws IOException {
    allocateBinaryLogClient().connect();
  }

  public void connect(long timeoutInMilliseconds) throws IOException, TimeoutException {
    allocateBinaryLogClient().connect(timeoutInMilliseconds);
  }

  public void process() {
    while(true) {
      Thread.yield();
    }
  }

  private synchronized BinaryLogClient allocateBinaryLogClient() {
    if (isConnected()) {
      throw new IllegalStateException("MySQL replication stream is already open");
    }
    binaryLogClient = new BinaryLogClient(hostname, port, username, password);
    binaryLogClient.registerEventListener(new DelegatingEventListener());
    return binaryLogClient;
  }


  public synchronized boolean isConnected() {
    return binaryLogClient != null && binaryLogClient.isConnected();
  }

  public synchronized void disconnect() throws IOException {
    if (binaryLogClient != null) {
      binaryLogClient.disconnect();
      binaryLogClient = null;
    }
  }


  private final class DelegatingEventListener implements BinaryLogClient.EventListener {
    TableMapEventData mapEvent = null; EventData crudEvent = null;

    @Override
    public void onEvent(Event event) {
      EventType eventType = event.getHeader().getEventType();
      switch (eventType) {
      case TABLE_MAP:
        mapEvent = (TableMapEventData)event.getData();
        //log.info("mapevent <{}>", mapEvent);
        break;
      case PRE_GA_WRITE_ROWS:
      case WRITE_ROWS:
      case EXT_WRITE_ROWS:
      case PRE_GA_UPDATE_ROWS:
      case UPDATE_ROWS:
      case EXT_UPDATE_ROWS:
      case PRE_GA_DELETE_ROWS:
      case DELETE_ROWS:
      case EXT_DELETE_ROWS:
        //log.info("crud {}", event);
        crudEvent = event.getData();
        break;
      case QUERY:
        break;
      case XID:
        if(mapEvent != null && crudEvent!= null) {
          EventHeaderV4 header = event.getHeader();
          MySQLChangeRecord evt = new MySQLChangeRecord(header, mapEvent, crudEvent);
          kdb.publish(evt);
        } else {
          log.info("skip e {}", event);
        }
        crudEvent = null;
        mapEvent = null;
      default:
      }
    }
  }

  public static void main(String[] args) throws Exception {
    MySQLReplicationStream stream = new MySQLReplicationStream(args[0], args[1]);
    stream.connect();
    stream.process();
  }

}
