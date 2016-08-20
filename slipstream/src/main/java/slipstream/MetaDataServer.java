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
  final static String uri = "table:slipstream";

  Session mdb;

  public MetaDataServer() {

  }

  public static class Schema {
    public String database;
    public String table;
    public LinkedHashMap<String, String> cols;
  }

  private void init(String db) {
    Util.checkDir(db);
    Connection conn = wiredtiger.open(db, "create");
    mdb = conn.open_session(null);
    mdb.create(uri, "key_format=SS,value_format=S");
  }

  public void run(PropertiesConfiguration config) {
    init(config.getString("metadata"));
    String[] schemas = config.getStringArray("schema");
    Gson gson = new Gson();
    Cursor c = mdb.open_cursor(uri, null, null);
    mdb.begin_transaction("isolation=snapshot");
    for(String schema : schemas) {
      Schema s = gson.fromJson(schema, Schema.class);
      log.info("init db={} table={}", s.database, s.table);
      c.putKeyString(s.database);
      c.putKeyString(s.table);
      c.putValueString(gson.toJson(s.cols));
      c.insert();
    }
    mdb.commit_transaction(null);
    log.info("Open your web browser and navigate to " + "http" + "://127.0.0.1:" + config.getInt("port") + '/');
    port(config.getInt("port"));
    get("/", (req, res) -> "MetaDataServer");
    get("/schema", (request, response) -> {
        String database = request.queryParams("database");
        String table = request.queryParams("table");
        Cursor cursor = mdb.open_cursor(uri, null, null);
        log.info("search db={} table={}", database, table);
        cursor.putKeyString(database);
        cursor.putKeyString(table);
        if(cursor.search() == 0) {
          return cursor.getValueString();
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
