package dstream;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class SdbSchemaFactory implements SchemaFactory {
  private static Logger log = LogManager.getLogger(SdbSchemaFactory.class);
  static final String ROWTIME_COLUMN_NAME = "ROWTIME";
  static Map<String, Tablet> shards;

  public static final SdbSchemaFactory INSTANCE = new SdbSchemaFactory();

  private SdbSchemaFactory() {
    shards = Tablet.getTablets("./datanode");
  }

  public Map<String, Tablet> getShards() {
    return shards;
  }

  public Schema create(SchemaPlus parentSchema, String name,
                       Map<String, Object> operand) {
    log.info("name {}", name);
    return new SdbSchema(this);
  }
}
