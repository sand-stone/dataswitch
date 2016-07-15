package slipstream;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
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

public class MySQLExtractorTest extends TestCase {
  private static Logger log = LogManager.getLogger(MySQLExtractorTest.class);
  
  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public MySQLExtractorTest( String testName ) {
    super( testName );
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite( MySQLExtractorTest.class );
  }

  public void testApp() {
    boolean f = false;
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
      MySQLExtractor extractor = (MySQLExtractor) wrapper.getExtractor();
      
      extractor.setBinlogFilePattern("slipstream");
      extractor.setBinlogDir("./src/test/data/");
      extractor.setDataSource("extractor");
      extractor.setLastEventId("000001:0");
      DBMSEvent evt = extractor.extract();
      log.info("event:{} {}",evt.getEventId(), evt.getData());
      for(DBMSData d : evt.getData()) {
        if(d instanceof RowChangeData) {
          RowChangeData r = (RowChangeData)d;
          for(OneRowChange chg : r.getRowChanges()) {
            log.info("chg:{}", chg.getAction());
          }
        }
      }      
    } catch(Exception e) {
      e.printStackTrace();
      f = true;
    }
    assertTrue( !f );
  }
}
