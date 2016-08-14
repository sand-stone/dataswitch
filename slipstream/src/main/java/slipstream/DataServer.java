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
import com.github.zk1931.jzab.PendingRequests;
import com.github.zk1931.jzab.PendingRequests.Tuple;
import com.github.zk1931.jzab.StateMachine;
import com.github.zk1931.jzab.Zab;
import com.github.zk1931.jzab.ZabConfig;
import com.github.zk1931.jzab.ZabException;
import com.github.zk1931.jzab.Zxid;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DataServer {
  private static Logger log = LogManager.getLogger(DataServer.class);
  private static String LogData = ".data#";
  private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();

  public DataServer() {
  }

  private static class Ring implements Runnable, StateMachine {
    private Zab zab;
    private String serverId;
    private final ZabConfig config = new ZabConfig();

    public Ring(String serverId, String joinPeer, String logDir) {
      try {
        this.serverId = serverId;
        if (this.serverId != null && joinPeer == null) {
          // It's the first server in cluster, joins itself.
          joinPeer = this.serverId;
        }
        if (this.serverId != null && logDir == null) {
          // If user doesn't specify log directory, default one is
          // serverId in current directory.
          logDir = this.serverId;
        }
        config.setLogDir(logDir);
        if (joinPeer != null) {
          zab = new Zab(this, config, this.serverId, joinPeer);
        } else {
          // Recovers from log directory.
          zab = new Zab(this, config);
        }
        this.serverId = zab.getServerId();
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
      log.debug("deliver a message: {}", clientId);
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
        return "writing to shard\n";
      });
    init();
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    Option port = OptionBuilder.withArgName("port")
      .hasArg(true)
      .isRequired(true)
      .withDescription("port number")
      .create("port");

    Option addr = OptionBuilder.withArgName("addr")
      .hasArg(true)
      .withDescription("addr (ip:port) for Zab.")
      .create("addr");

    Option join = OptionBuilder.withArgName("join")
      .hasArg(true)
      .withDescription("the addr of server to join.")
      .create("join");

    Option dir = OptionBuilder.withArgName("dir")
      .hasArg(true)
      .withDescription("the directory for logs.")
      .create("dir");

    Option help = OptionBuilder.withArgName("h")
      .hasArg(false)
      .withLongOpt("help")
      .withDescription("print out usages.")
      .create("h");

    options.addOption(port)
      .addOption(addr)
      .addOption(join)
      .addOption(dir)
      .addOption(help);

    CommandLineParser parser = new BasicParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("DataServer", options);
        return;
      }
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("DataServer", options);
      return;
    }

    new DataServer().run(Integer.parseInt(cmd.getOptionValue("port")),
                         cmd.getOptionValue("addr"),
                         cmd.getOptionValue("join"),
                         cmd.getOptionValue("dir"));
  }

}
