package slipstream;

import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.StandardWatchEventKinds;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.ByteArrayEventData;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ByteArrayEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import java.util.concurrent.Future;

import slipstream.MySQLChangeRecord.FieldType;
import slipstream.MySQLChangeRecord.OperationType;

class MySQLBinLogReader {
  private static Logger log = LogManager.getLogger(MySQLBinLogReader.class);

  private KdbConnector kdb;

  public MySQLBinLogReader(List<String> uris, String table) {
    kdb = new KdbConnector(uris, table);
  }

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
    String schema="{\"uri\":\"acme\", \"database\":\"acme\",\"table\":\"employees\", \"cols\":{\"id\":\"int\",\"first\":\"string\",\"last\":\"string\",\"age\":\"int\"}}";
    log.info("evt {}=>{} : {} ", evt, MySQLChangeRecord.get(evt.key(), evt.value()), evt.toSQL(schema));
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    keys.add(evt.key());
    values.add(evt.value());
    kdb.write(keys, values);
  }

  public void process(InputStream input) throws IOException {
    EventDeserializer eventDeserializer = new EventDeserializer();
    eventDeserializer.setEventDataDeserializer(EventType.XID, new ByteArrayEventDataDeserializer());
    eventDeserializer.setEventDataDeserializer(EventType.QUERY, new ByteArrayEventDataDeserializer());
    BinaryLogFileReader reader = new BinaryLogFileReader(input, eventDeserializer);
    try {
      TableMapEventData mapEvent = null; EventData crudEvent = null;
      for (Event dbevent; (dbevent = reader.readEvent()) != null; ) {
        EventType eventType = dbevent.getHeader().getEventType();
        switch(eventType) {
        case TABLE_MAP:
          mapEvent = (TableMapEventData)dbevent.getData();
          break;
        case EXT_WRITE_ROWS:
        case EXT_UPDATE_ROWS:
        case EXT_DELETE_ROWS:
          crudEvent = dbevent.getData();
          break;
        case XID:
          EventHeaderV4 header = dbevent.getHeader();
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
          crudEvent = null;
          mapEvent = null;
        }
      }
    } finally {
      reader.close();
    }
    log.info("total evts {}", kdb.count());
  }

  public static void main(String[] args) throws Exception {
    FileInputStream in = new FileInputStream(args[0]);
    new MySQLBinLogReader(Arrays.asList(args[1]), "mysqlevents").process(in);
  }
}
