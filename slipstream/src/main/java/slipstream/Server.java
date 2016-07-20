package slipstream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.variables.DistributedValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class Server {
  private static Logger log = LogManager.getLogger(Server.class);
  private static String LogData = ".data#";
  public Server() {
  }

  public void run(String role, String location) {
    String[] parts = location.split(":");
    Address address = new Address(parts[0], Integer.valueOf(parts[1]));
    AtomixReplica.Builder builder = AtomixReplica.builder(address);
    AtomixReplica atomix = AtomixReplica.builder(address)
      .withTransport(new NettyTransport())
      .withStorage(Storage.builder()
                   .withStorageLevel(StorageLevel.DISK)
                   .withDirectory(LogData+role)
                   .withMinorCompactionInterval(Duration.ofSeconds(30))
                   .withMajorCompactionInterval(Duration.ofMinutes(1))
                   .withMaxSegmentSize(1024*1024*8)
                   .withMaxEntriesPerSegment(1024*8)
                   .build())
      .build();

    atomix.bootstrap().join();
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      System.out.println("Sever master/salve localhost:8000");
      return;
    }
    
    new Server().run(args[0], args[1]);
    log.info("server started");
  }
}
