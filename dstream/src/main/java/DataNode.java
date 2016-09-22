package dstream;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import static spark.Spark.*;
import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;

public final class DataNode {
  private static Logger log = LogManager.getLogger(DataNode.class);

  private static final String rootData = "./datanode";

  public static void main(String[] args) {
    int maxThreads = 8;
    int minThreads = 2;
    int timeOutMillis = 30000;
    int port = 8000;
    port(port);
    threadPool(maxThreads, minThreads, timeOutMillis);
    HashMap<String, Tablet> shards = new HashMap<String, Tablet>();
    get("/", (req, res) -> "DStream DataNode");
    post("/createtable", (request, response) -> {
        byte[] data = request.bodyAsBytes();
        Message.CreateTable msg = (Message.CreateTable)Serializer.deserialize(data);
        Tablet tablet = new Tablet(rootData, msg.table);
        shards.put(msg.table.getName(), tablet);
        log.info("msg {}", msg);
        return "create table\n";
      });
    post("/upsertable", (request, response) -> {
        byte[] data = request.bodyAsBytes();
        Message.UpsertTable msg = (Message.UpsertTable)Serializer.deserialize(data);
        Tablet tablet = shards.get(msg.table);
        try(Tablet.Context ctx = tablet.getContext()) {
          try {
            tablet.upsert(ctx, msg);
          } catch(Exception ex) {
            log.info(ex.toString());
          }
        }
        log.info("msg {}", msg);
        return "upsert table\n";
      });
    post("/deletetable", (request, response) -> {
        byte[] data = request.bodyAsBytes();
        Message.DeleteTable msg = (Message.DeleteTable)Serializer.deserialize(data);
        log.info("msg {}", msg);
        return "delete table\n";
      });
    post("/querytable", (request, response) -> {
        byte[] data = request.bodyAsBytes();
        Message.QueryTable msg = (Message.QueryTable)Serializer.deserialize(data);
        log.info("msg {}", msg);
        return "query table\n";
      });
    init();
  }

}
