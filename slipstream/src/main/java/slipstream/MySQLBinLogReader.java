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

  public MySQLBinLogReader() {
    kdb = KdbConnector.get();
  }

  public void process(InputStream input) throws IOException {
    EventDeserializer eventDeserializer = new EventDeserializer();
    eventDeserializer.setEventDataDeserializer(EventType.XID, new ByteArrayEventDataDeserializer());
    eventDeserializer.setEventDataDeserializer(EventType.QUERY, new ByteArrayEventDataDeserializer());
    BinaryLogFileReader reader = new BinaryLogFileReader(input, eventDeserializer);
    try {
      TableMapEventData mapEvent = null; EventData crudEvent = null;
      for (Event event; (event = reader.readEvent()) != null; ) {
        EventType eventType = event.getHeader().getEventType();
        switch(eventType) {
        case TABLE_MAP:
          mapEvent = (TableMapEventData)event.getData();
          break;
        case EXT_WRITE_ROWS:
        case EXT_UPDATE_ROWS:
        case EXT_DELETE_ROWS:
          crudEvent = event.getData();
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
        }
      }
    } finally {
      reader.close();
    }
    log.info("total evts {}", kdb.count());
  }

  public static void main(String[] args) throws Exception {
    FileInputStream in = new FileInputStream(args[0]);
    new MySQLBinLogReader().process(in);
  }
}
