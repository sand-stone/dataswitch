package slipstream;

import java.net.*;
import java.time.Duration;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import slipstream.paxos.*;
import slipstream.paxos.communication.*;
import slipstream.paxos.fragmentation.*;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import static spark.Spark.*;
import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;

public class DataServer {
  private static Logger log = LogManager.getLogger(DataServer.class);
  private static String LogData = ".data#";
  private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();

  public DataServer() {
  }

  private static class Ring implements Runnable, Receiver {
    private String[] locations;
    private int id;

    public Ring(int id, String[] locations) {
      this.id = id;
      this.locations = locations;
    }

    public void receive(Serializable message) {
      System.out.println("received " + message.toString());
    }

    public void run() {
      Thread.currentThread().setName("data server ring monitor");
      FragmentingGroup group = null;
      try {
        Member[] members = new Member[locations.length];
        int i = 0;
        for(String location : locations) {
          log.info("location {}", location);
          String[] parts = location.split(":");
          members[i++] = new Member(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1]));
        }
        Members council = new Members(Arrays.asList(members));
        group = new FragmentingGroup(council.get(id), this);
        log.info("data server ring monitor started {}", group);
        while (true) {
          group.broadcast("Ping from server " + id);
          Thread.sleep(5000);
        }
      } catch(Exception e) {
        log.info(e);
      } finally {
        if(group!= null)
          group.close();
      }
    }

  }

  @OnWebSocketConnect
  public void connected(Session session) {
    sessions.add(session);
  }

  @OnWebSocketClose
  public void closed(Session session, int statusCode, String reason) {
    sessions.remove(session);
  }

  @OnWebSocketMessage
  public void message(Session session, String message) throws IOException {
    System.out.println("Got: " + message);   // Print message
    session.getRemote().sendString(message); // and send it back
  }

  public void run(int id, int port, String[] locations) {
    Ring ring = new Ring(id, locations);
    Thread rt = new Thread(ring);
    rt.start();
    port(port);
    webSocket("/echo", DataServer.class);
    int maxThreads = 8;
    int minThreads = 2;
    int timeOutMillis = 30000;
    threadPool(maxThreads, minThreads, timeOutMillis);
    get("/echo", (req, res) -> "Hello World from Slipstream");
    post("/mysql", (request, response) -> {
        return "writing to shard\n";
      });
    init();
  }

  public static void main(String[] args) throws Exception {
    if(args.length<1) {
      System.out.println("java -cp slipstream.jar slipstream.DataSever conf/dataserverX.properties");
      return;
    }
    Configurations configs = new Configurations();
    File propertiesFile = new File(args[0]);
    PropertiesConfiguration config = configs.properties(propertiesFile);
    log.info("config {} {}", config.getInt("serverid"),config.getStringArray("members"));
    new DataServer().run(config.getInt("serverid"), config.getInt("port"), config.getStringArray("members"));
  }

}
