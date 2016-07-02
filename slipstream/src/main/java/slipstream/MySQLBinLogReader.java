package slipstream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.ByteArrayEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.ByteArrayEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class MySQLBinLogReader {
  private static Logger log = LogManager.getLogger(MySQLBinLogReader.class);

  public MySQLBinLogReader() {
  }

  public static void main(String[] args) throws Exception {
    EventDeserializer eventDeserializer = new EventDeserializer();
    eventDeserializer.setEventDataDeserializer(EventType.XID, new ByteArrayEventDataDeserializer());
    eventDeserializer.setEventDataDeserializer(EventType.QUERY, new ByteArrayEventDataDeserializer());
    BinaryLogFileReader reader = new BinaryLogFileReader(new FileInputStream(args[0]), eventDeserializer);
    try {
      boolean n = true, b = true;
      for (Event event; (event = reader.readEvent()) != null && (n || b); ) {
        EventType eventType = event.getHeader().getEventType();
        if (eventType == EventType.XID) {
          log.info(event.getData().toString());
        } else
          log.info(eventType);
        if(event.getData()!=null)
          log.info(event.getData().toString());
      }
    } finally {
      reader.close();
    }
  }
}
