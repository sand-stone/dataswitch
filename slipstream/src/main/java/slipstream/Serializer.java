package slipstream;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public final class Serializer {
  private static Logger log = LogManager.getLogger(Serializer.class);

  private Serializer() {
  }

  public static ByteBuffer serialize(Object msg) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(msg);
      oos.close();
      return ByteBuffer.wrap(bos.toByteArray());
    }
  }

  public static Object deserialize(ByteBuffer bb) {
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      return ois.readObject();
    } catch (ClassNotFoundException|IOException ex) {
      log.error("Failed to deserialize: {}", bb, ex);
      throw new RuntimeException("Failed to deserialize ByteBuffer");
    }
  }
  
}
