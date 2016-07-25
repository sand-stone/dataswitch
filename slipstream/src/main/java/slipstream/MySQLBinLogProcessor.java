package slipstream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import slipstream.replicator.extractor.mysql.*;
import slipstream.replicator.commons.common.config.TungstenProperties;
import slipstream.replicator.ReplicatorException;
import slipstream.replicator.applier.DummyApplier;
import slipstream.replicator.conf.ReplicatorConf;
import slipstream.replicator.conf.ReplicatorMonitor;
import slipstream.replicator.conf.ReplicatorRuntime;
import slipstream.replicator.datasource.AliasDataSource;
import slipstream.replicator.extractor.ExtractorWrapper;
import slipstream.replicator.management.MockOpenReplicatorContext;
import slipstream.replicator.pipeline.Pipeline;
import slipstream.replicator.pipeline.SingleThreadStageTask;
import slipstream.replicator.event.*;
import slipstream.replicator.dbms.*;
import java.io.FileInputStream;
import java.nio.file.*;
import java.nio.file.StandardWatchEventKinds;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.ByteArrayEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.ByteArrayEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;

class MySQLBinLogProcessor implements Runnable {
  private static Logger log = LogManager.getLogger(MySQLBinLogProcessor.class);

  MySQLExtractor extractor;

  public MySQLBinLogProcessor() {
    init();
  }

  private void init() {
     try {
      TungstenProperties conf = new TungstenProperties();
      conf.setString(ReplicatorConf.SERVICE_NAME, "test");
      conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);
      conf.setString(ReplicatorConf.PIPELINES, "master");
      conf.setString(ReplicatorConf.PIPELINE_ROOT + ".master", "extract");
      conf.setString(ReplicatorConf.STAGE_ROOT + ".extract",
                     SingleThreadStageTask.class.toString());
      conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.extractor",
                     "mysql");
      conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.applier", "dummy");
      conf.setString(ReplicatorConf.APPLIER_ROOT + ".dummy",
                     DummyApplier.class.getName());
      conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql",
                     MySQLExtractor.class.getName());

      ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                                                        new MockOpenReplicatorContext(),
                                                        ReplicatorMonitor.getInstance());
      runtime.configure();
      Pipeline p = runtime.getPipeline();
      ExtractorWrapper wrapper = (ExtractorWrapper) p.getStages().get(0).getExtractor0();
      extractor = (MySQLExtractor) wrapper.getExtractor();
     } catch (Exception e) {
       log.info(e);
     }
  }

  public void run2() {
    try {
      Path dir = Paths.get(SlipstreamServer.MySQLLogFileRoot);
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
          extractor.setBinlogFilePattern(segs[0]);
          extractor.setBinlogDir(SlipstreamServer.MySQLLogFileRoot);
          extractor.setDataSource("extractor");
          extractor.setLastEventId("000001:0");
          while(true) {
            DBMSEvent evt = extractor.extract();
            log.info("event:{}",evt);
            if(evt == null)
              break;
            log.info("event:{} {}",evt.getEventId(), evt.getData());
            for(DBMSData d : evt.getData()) {
              if(d instanceof RowChangeData) {
                RowChangeData r = (RowChangeData)d;
                for(OneRowChange chg : r.getRowChanges()) {
                  log.info("chg:{}", chg.getAction());
                }
              }
            }
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

    public void run() {
    try {
      Path dir = Paths.get(SlipstreamServer.MySQLLogFileRoot);
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
          BinaryLogFileReader reader = new BinaryLogFileReader(new FileInputStream(SlipstreamServer.MySQLLogFileRoot+filename.toString()), eventDeserializer);
          try {
            boolean n = true, b = true;
            for (Event dbevent; (dbevent = reader.readEvent()) != null && (n || b); ) {
              EventType eventType = dbevent.getHeader().getEventType();
              if (eventType == EventType.XID) {
                log.info(dbevent.getData().toString());
              } else
                log.info(eventType);
              if(dbevent.getData()!=null)
                log.info(dbevent.getData().toString());
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

}
