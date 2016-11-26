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

  public MySQLReplicationStream(String username, String password) {
    this("localhost", 3306, username, password);
  }

  public MySQLReplicationStream(String hostname, int port, String username, String password) {
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;
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

    private final Map<Long, TableMapEventData> tablesById = new HashMap<Long, TableMapEventData>();
    private boolean transactionInProgress;

    private FieldType[] getFieldTypes(TableMapEventData mapEvent) {
      byte[] types = mapEvent.getColumnTypes();
      FieldType[] ftypes = new FieldType[types.length];
      for(int i = 0; i < types.length; i++) {
        ftypes[i] = FieldType.map(types[i]);
      }
      return ftypes;
    }

    private OperationType getOpType(EventData evt) {
      OperationType ret = OperationType.None;
      if (evt instanceof WriteRowsEventData) {
        ret = OperationType.Insert;
      } else if (evt instanceof UpdateRowsEventData) {
        ret = OperationType.Update;
      }
      return ret;
    }

    private Object[] getRows(EventData evt) {
      Object[] rows = null;
      if(evt instanceof UpdateRowsEventData)
        rows = ((UpdateRowsEventData)evt).getRows().toArray();
      else if(evt instanceof WriteRowsEventData)
        rows = ((WriteRowsEventData)evt).getRows().toArray();
      return rows;
    }

    private BitSet getIncludedCols(EventData evt) {
      BitSet ret = null;
      if(evt instanceof UpdateRowsEventData)
        ret = ((UpdateRowsEventData)evt).getIncludedColumns();
      else if(evt instanceof WriteRowsEventData)
        ret = ((WriteRowsEventData)evt).getIncludedColumns();
      return ret;
    }

    private void send(MySQLChangeRecord evt) {
      log.info("evt {}=>{}", evt, MySQLChangeRecord.get(evt.key(), evt.value()));
      /*List<byte[]> keys = new ArrayList<byte[]>();
        List<byte[]> values = new ArrayList<byte[]>();
        keys.add(evt.key());
        values.add(evt.value());
        kdb.write(keys, values);*/
    }

    @Override
    public void onEvent(Event event) {
      // todo: do something about schema changes
      EventType eventType = event.getHeader().getEventType();
      TableMapEventData mapEvent = null; EventData crudEvent = null;
      switch (eventType) {
      case TABLE_MAP:
        mapEvent = (TableMapEventData)event.getData();
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
        log.info("crud {}", event);
        crudEvent = event.getData();
        break;
      case QUERY:
        break;
      case XID:
        if(mapEvent != null) {
          EventHeaderV4 header = event.getHeader();
          MySQLChangeRecord evt = new MySQLChangeRecord(header.getServerId(),
                                                        mapEvent.getDatabase(),
                                                        mapEvent.getTable(),
                                                        getOpType(crudEvent),
                                                        header.getTimestamp(),
                                                        header.getNextPosition(),
                                                        getIncludedCols(crudEvent),
                                                        getFieldTypes(mapEvent),
                                                        getRows(crudEvent));
          send(evt);
        } else {
          log.info("e {}", event);
        }
        crudEvent = null;
        mapEvent = null;
      default:
      }
    }
  }

  public static void main(String[] args) throws Exception {
    MySQLReplicationStream stream = new MySQLReplicationStream("root", "yourpassword");
    stream.connect();
    stream.process();
  }

}
