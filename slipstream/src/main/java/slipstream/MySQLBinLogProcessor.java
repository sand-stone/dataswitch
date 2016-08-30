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
import org.asynchttpclient.*;
import java.util.concurrent.Future;
import slipstream.DBTransactionEvent.FieldType;
import slipstream.DBTransactionEvent.OperationType;

class MySQLBinLogProcessor {
  private static Logger log = LogManager.getLogger(MySQLBinLogProcessor.class);
  final AsyncHttpClientConfig config;
  AsyncHttpClient client;
  String[] urls;

  public MySQLBinLogProcessor(String[] urls) {
    config = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(Integer.MAX_VALUE).build();
    client = new DefaultAsyncHttpClient(config);
    this.urls = urls;
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
          DBTransactionEvent evt = new DBTransactionEvent(header.getServerId(),
                                                          mapEvent.getDatabase(),
                                                          mapEvent.getTable(),
                                                          getOpType(crudEvent),
                                                          header.getTimestamp(),
                                                          header.getNextPosition(),
                                                          getIncludedCols(crudEvent),
                                                          getFieldTypes(mapEvent),
                                                          getRows(crudEvent));
          //log.info("transaction {}", evt);
          sendMsg(evt);
          crudEvent = null;
          mapEvent = null;
        }
      }
    } finally {
      reader.close();
    }
  }

  private void sendMsg(Message evt) throws IOException {
    ByteBuffer data = Serializer.serialize(evt);
    Response r;
    try {
      r=client.preparePost(urls[0])
        .setBody(data.array())
        .execute()
        .get();
      log.info("r: {}", r);
    } catch(Exception e) {
      log.info(e);
    }
  }

}
