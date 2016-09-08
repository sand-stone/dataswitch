package slipstream;

import static spark.Spark.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.gson.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import javax.servlet.MultipartConfigElement;
import javax.servlet.http.*;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import com.wiredtiger.db.*;

public class MetaDataServer {
  private static Logger log = LogManager.getLogger(MetaDataServer.class);

  MetaDataTable mdb;

  public MetaDataServer() {

  }

  private void init(String db) {
    mdb = new MetaDataTable(db);
  }

  public void run(PropertiesConfiguration config) {
    init(config.getString("metadata"));
    String[] schemas = config.getStringArray("schema");
    Gson gson = new Gson();
    try (MetaDataTable.Context cxt = mdb.getContext("schema")) {
      for(String schema : schemas) {
        Schema.TableSchema s = gson.fromJson(schema, Schema.TableSchema.class);
        log.info("init db={} table={}", s.database, s.table);
        cxt.insert(s.database, s.table, gson.toJson(s.cols));
      }
    }

    log.info("Open your web browser and navigate to " + "http" + "://127.0.0.1:" + config.getInt("port") + '/');
    port(config.getInt("port"));
    get("/", (req, res) -> "This is MetaDataServer micro service");
    get("/schema", (request, response) -> {
        String database = request.queryParams("database");
        String table = request.queryParams("table");
        try (MetaDataTable.Context cxt = mdb.getContext("schema")) {
          String ret = (String)cxt.search(database, table);
          if(ret != null) {
            return ret;
          }
        }
        return "Slipstream does not have the schema\n";
      });
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 1) {
      System.out.println("java -cp ./target/slipstream-1.0-SNAPSHOT.jar slipstream.MetaDataServer conf/metadata.properties");
      return;
    }
    Configurations configs = new Configurations();
    File propertiesFile = new File(args[0]);
    PropertiesConfiguration config = configs.properties(propertiesFile);
    new MetaDataServer().run(config);
  }
}
