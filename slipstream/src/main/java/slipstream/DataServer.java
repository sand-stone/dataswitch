package slipstream;

import java.net.*;
import java.time.Duration;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import static spark.Spark.*;
import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;
import slipstream.jzab.PendingRequests;
import slipstream.jzab.PendingRequests.Tuple;
import slipstream.jzab.StateMachine;
import slipstream.jzab.Zab;
import slipstream.jzab.ZabConfig;
import slipstream.jzab.ZabException;
import slipstream.jzab.Zxid;

public class DataServer {
  private static Logger log = LogManager.getLogger(DataServer.class);
  private static String LogData = ".data#";
  private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();
  private String dataDir;
  private String serverid;

  public DataServer(String serverid, String dataDir) {
    this.serverid = serverid;
    this.dataDir = dataDir;
  }

  private class Ring implements Runnable, StateMachine {
    private Zab zab;
    private String serverId;
    private final ZabConfig config = new ZabConfig();
    private Shard shard;

    public Ring(String serverId, String joinPeer, String logDir) {
      try {
        this.serverId = serverId;
        if (this.serverId != null && joinPeer == null) {
          // It's the first server in cluster, joins itself.
          joinPeer = this.serverId;
        }
        if (this.serverId != null && logDir == null) {
          logDir = this.serverId;
        }
        config.setLogDir(logDir);
        File logdata = new File(logDir);
        if (!logdata.exists()) {
          zab = new Zab(this, config, this.serverId, joinPeer);
        } else {
          // Recovers from log directory.
          zab = new Zab(this, config);
        }
        this.serverId = zab.getServerId();
        shard = new Shard(DataServer.this.dataDir+File.separator+DataServer.this.serverid);
      } catch (Exception ex) {
        log.error("Caught exception : ", ex);
        throw new RuntimeException();
      }
    }

    @Override
    public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
      log.debug("Preprocessing a message: {}", message);
      return message;
    }

    @Override
    public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                        Object ctx) {
      Object o = Serializer.deserialize(stateUpdate);
      shard.write(o);
    }

    @Override
    public void flushed(Zxid zxid, ByteBuffer request, Object ctx) {
      log.debug("flush {} message: {}", zxid, ctx);
    }

    @Override
    public void save(FileOutputStream fos) {
      log.debug("save snapshot");
    }

    @Override
    public void restore(FileInputStream fis) {
      log.debug("restore snapshot");
    }

    @Override
    public void snapshotDone(String filePath, Object ctx) {
    }

    @Override
    public void removed(String peerId, Object ctx) {
    }

    @Override
    public void recovering(PendingRequests pendingRequests) {
      log.info("Recovering...");
      // Returns error for all pending requests.
      for (Tuple tp : pendingRequests.pendingSends) {
        log.info("tuple {}", tp);
      }
    }

    @Override
    public void leading(Set<String> activeFollowers, Set<String> clusterMembers) {
      log.info("LEADING with active followers : ");
      for (String peer : activeFollowers) {
        log.info(" -- {}", peer);
      }
      log.info("Cluster configuration change : ", clusterMembers.size());
      for (String peer : clusterMembers) {
        log.info(" -- {}", peer);
      }
    }

    @Override
    public void following(String leader, Set<String> clusterMembers) {
      log.info("FOLLOWING {}", leader);
      log.info("Cluster configuration change : ", clusterMembers.size());
      for (String peer : clusterMembers) {
        log.info(" -- {}", peer);
      }
    }

    public void run() {
      try {
      } catch(Exception e) {
        log.info(e);
      } finally {
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

  public void run(int port, String serverId, String joinPeer, String logDir) {
    Ring ring = new Ring(serverId, joinPeer, logDir);
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
        byte[] data = request.bodyAsBytes();
        ring.zab.send(ByteBuffer.wrap(data), null);
        return "writing to shard\n";
      });
    init();
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 1) {
      System.out.println("java -cp ./target/slipstream-1.0-SNAPSHOT.jar slipstream.DataServer conf/dataserverX.properties");
      return;
    }
    File propertiesFile = new File(args[0]);
    if(!propertiesFile.exists()) {
      System.out.printf("config file %s does not exist", propertiesFile.getName());
      return;
    }
    Configurations configs = new Configurations();
    PropertiesConfiguration config = configs.properties(propertiesFile);
    new DataServer(config.getString("serverid"), config.getString("dataDir")).run(config.getInt("port"),
                                                                                  config.getString("ringaddr"),
                                                                                  config.getString("leader"),
                                                                                  config.getString("logDir"));
  }

}
