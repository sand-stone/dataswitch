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

class MySQLBinLogProcessor implements Runnable {
  private static Logger log = LogManager.getLogger(MySQLBinLogProcessor.class);
  final AsyncHttpClientConfig config;
  AsyncHttpClient client;
  String[] urls;

  public MySQLBinLogProcessor(String[] urls) {
    config = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(Integer.MAX_VALUE).build();
    client = new DefaultAsyncHttpClient(config);
    this.urls = urls;
  }

  public void run() {
    try {
      Path dir = Paths.get(GatewayServer.MySQLLogFileRoot);
      WatchService watchService = FileSystems.getDefault().newWatchService();
      dir.register(watchService,
                   StandardWatchEventKinds.ENTRY_CREATE,
                   StandardWatchEventKinds.ENTRY_DELETE,
                   StandardWatchEventKinds.ENTRY_MODIFY);
      log.info("MySQLBinLogProcessor running");
      for(;;) {
        WatchKey key = watchService.poll();
        if(key== null)
          continue;
        for(WatchEvent<?> event: key.pollEvents()) {
          WatchEvent.Kind<?> kind = event.kind();
          if (kind == StandardWatchEventKinds.OVERFLOW) {
            continue;
          }

          WatchEvent<Path> ev = (WatchEvent<Path>)event;
          Path filename = ev.context();
          String[] segs = filename.toString().split("\\.");
          if(segs.length != 2)
            continue;
          if(segs[1].equals("index")) {
            log.info("no need to process index file {}", filename.toString());
            continue;
          }
          log.info("process file {}", filename.toString());
          EventDeserializer eventDeserializer = new EventDeserializer();
          eventDeserializer.setEventDataDeserializer(EventType.XID, new ByteArrayEventDataDeserializer());
          eventDeserializer.setEventDataDeserializer(EventType.QUERY, new ByteArrayEventDataDeserializer());
          BinaryLogFileReader reader = new BinaryLogFileReader(new FileInputStream(GatewayServer.MySQLLogFileRoot+filename.toString()), eventDeserializer);
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
                MySQLTransactionEvent evt = new MySQLTransactionEvent(header.getServerId(),
                                                                      mapEvent.getDatabase(),
                                                                      mapEvent.getTable(),
                                                                      header.getTimestamp(),
                                                                      header.getNextPosition(),
                                                                      mapEvent,
                                                                      crudEvent);
                //log.info("transaction {}", evt);
                sendMsg(evt);
                crudEvent = null;
                mapEvent = null;
              }
            }
          } finally {
            reader.close();
          }
          log.info("done processing file {}", filename.toString());
        }
        boolean valid = key.reset();
        if (!valid) {
          break;
        }
      }
      watchService.close();
    } catch(Exception e) {
      e.printStackTrace();
    }
    log.info("MySQLBinLogProcessor stops");
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
