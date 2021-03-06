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
    SdbSchemaFactory.get().getTables();
    get("/", (req, res) -> "DStream DataNode");
    post("/createtable", (request, response) -> {
        try {
          byte[] data = request.bodyAsBytes();
          Message.CreateTable msg = (Message.CreateTable)Serializer.deserialize(data);
          SdbSchemaFactory.get().getTables();
          SdbSchemaFactory.get().addTable(msg);
          SdbSchemaFactory.get().getTables();
          log.info("msg {}", msg);
          return "create table\n";
        } catch(Exception e) {
          e.printStackTrace();
          log.info(e.toString());
          throw e;
        }
      });
    post("/upsertable", (request, response) -> {
        try {
          byte[] data = request.bodyAsBytes();
          Message.UpsertTable msg = (Message.UpsertTable)Serializer.deserialize(data);
          log.info("msg {}", msg);
          String table = request.queryParams("table");
          if(table == null)
            return "table not found";
          String pid = request.queryParams("partition");
          Tablet tablet = SdbSchemaFactory.get().getTablet(table, pid == null? 0 : Integer.parseInt(pid));
          try(Tablet.Context ctx = tablet.getContext()) {
            try {
              tablet.upsert(ctx, msg);
            } catch(Exception ex) {
              log.info(ex.toString());
            }
          }
          return "upsert table";
        }  catch(Exception e) {
          e.printStackTrace();
          log.info(e.toString());
          throw e;
        }
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
        log.info("msg {}", msg.table);
        Expression.WireSerializedLambda f = Expression.WireSerializedLambda.read(ByteBuffer.wrap(msg.expr));
        Tablet tablet = SdbSchemaFactory.get().getTablet(msg.table, 0);
        try(Tablet.Context ctx = tablet.getContext()) {
          try {
            tablet.filter(ctx, f);
          } catch(Exception ex) {
            log.info(ex.toString());
          }
        }
        return "query table\n";
      });
    init();
  }

}
