package dstream;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import com.wiredtiger.db.*;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class SdbSchemaFactory implements SchemaFactory {
  private static Logger log = LogManager.getLogger(SdbSchemaFactory.class);
  static final String ROWTIME_COLUMN_NAME = "ROWTIME";
  static Map<String, Tablet> cache;
  private Connection conn;
  private String db;
  private static final String tnx = "isolation=snapshot";
  private final String dataDir = "./data";
  private final String metaDir = "./meta";
  private final String dbconfig = "create";

  private static final SdbSchemaFactory instance = new SdbSchemaFactory();

  public static SdbSchemaFactory get() {
    return instance;
  }

  private SdbSchemaFactory() {
    Utils.checkDir(metaDir);
    conn = wiredtiger.open(metaDir, dbconfig);
    Session session = conn.open_session(null);
    session.create("table:metabase", "key_format=Si,value_format=Su,columns=(name,pid,location,schema)");
    session.close(null);
    cache = new HashMap<String, Tablet>();
  }

  public List<Tablet> getTablet(String table) {
    List<Tablet> tablets = new ArrayList<Tablet>();
    Session session = conn.open_session(null);
    Cursor cursor = session.open_cursor("table:metabase", null, null);
    cursor.putKeyString(table);
    if(cursor.search_near() == SearchStatus.LARGER) {
      do {
        String location = cursor.getValueString();
        Table tbl = (Table)Serializer.deserialize(cursor.getValueByteArray());
        Tablet tablet = new Tablet(location, tbl);
        tablets.add(tablet);
      } while(cursor.next() == 0);
    }
    session.close(null);
    return tablets;
  }

  public Tablet getTablet(String name, int pid) {
    Tablet tablet = null;
    Session session = conn.open_session(null);
    Cursor cursor = session.open_cursor("table:metabase", null, null);
    cursor.putKeyString(name);
    cursor.putKeyInt(pid);
    if(cursor.search() == 0) {
      String location = cursor.getValueString();
      Table tbl = (Table)Serializer.deserialize(cursor.getValueByteArray());
      tablet = new Tablet(location, tbl);
      cache.put(name, tablet);
    }
    session.close(null);
    return tablet;
  }

  public void addTable(Message.CreateTable msg) {
    Session session = conn.open_session(null);
    session.begin_transaction(tnx);
    try {
      Cursor cursor = session.open_cursor("table:metabase", null, null);
      for(int p = 0; p < msg.shards; p++) {
        cursor.putKeyString(msg.table.getName());
        cursor.putKeyInt(p);
        cursor.putValueString(dataDir+File.separator+msg.table.getName()+File.separator+p);
        cursor.putValueByteArray(Serializer.serialize(msg.table).array());
        cursor.insert();
      }
      session.commit_transaction(null);
    } catch(WiredTigerRollbackException e) {
      session.rollback_transaction(tnx);
    }
    session.close(null);
  }

  public Map<String, Tablet> getShards() {
    return cache;
  }

  public Schema create(SchemaPlus parentSchema, String name,
                       Map<String, Object> operand) {
    log.info("name {}", name);
    return new SdbSchema(this);
  }
}
