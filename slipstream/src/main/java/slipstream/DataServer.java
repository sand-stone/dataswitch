package slipstream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import slipstream.paxos.*;
import slipstream.paxos.communication.*;
import slipstream.paxos.fragmentation.*;
import java.io.Serializable;
import java.net.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class DataServer {
  private static Logger log = LogManager.getLogger(DataServer.class);
  private static String LogData = ".data#";

  public static class MyReceiver implements Receiver {
    // we follow a reactive pattern here
    public void receive(Serializable message) {
      System.out.println("received " + message.toString());
    }
  };

  public DataServer() {
  }

  public void run(String role, String location) throws Exception {
    //String[] parts = location.split(":");
    // this is the list of members
    Members members = new Members(
                                  new Member(), // this is a reference to a member on the localhost on default port (2440)
                                  new Member(2441), // this one is on localhost with the specified port
                                  new Member(InetAddress.getLocalHost(), 2442)); // you can specify the address and port manually

    // this actually creates the members
    FragmentingGroup group1 = new FragmentingGroup(members.get(0), new MyReceiver());
    FragmentingGroup group2 = new FragmentingGroup(members.get(1), new MyReceiver());
    FragmentingGroup group3 = new FragmentingGroup(members.get(2), new MyReceiver());

    // this will cause all receivers to print "received Hello"
    group2.broadcast("Hello");

    Thread.sleep(1); // allow the members to receive the message

    group1.close(); group2.close(); group3.close();
  }

  public static void main(String[] args) throws Exception {
    /*if(args.length < 2) {
      System.out.println("Sever master/salve localhost:8000");
      return;
      }*/

    //new Server().run(args[0], args[1]);
    new DataServer().run(null, null);
    log.info("server started");
  }
}
