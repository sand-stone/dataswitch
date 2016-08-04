package slipstream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import slipstream.paxos.*;
import slipstream.paxos.communication.*;
import slipstream.paxos.fragmentation.*;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.io.*;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;

public class DataServer {
  private static Logger log = LogManager.getLogger(DataServer.class);
  private static String LogData = ".data#";

  public static class DataHandler implements Receiver {
    public void receive(Serializable message) {
      System.out.println("received " + message.toString());
    }
  };

  public DataServer() {
  }

  public void run(int id, String[] locations) throws Exception {
    Member[] members = new Member[locations.length];
    int i = 0;
    for(String location : locations) {
      log.info("location {}", location);
      String[] parts = location.split(":");
      members[i++] = new Member(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1]));
    }

    Members council = new Members(Arrays.asList(members));

    FragmentingGroup group = new FragmentingGroup(council.get(id), new DataHandler());

    log.info("server started {}", group);
    try {
      while (true) {
        group.broadcast("Ping from server " + id);
        Thread.sleep(5000);
      }
    } finally {
      group.close();
    }
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
    new DataServer().run(config.getInt("serverid"), config.getStringArray("members"));
  }
}
