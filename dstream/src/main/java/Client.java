package dstream;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.asynchttpclient.*;
import java.util.concurrent.Future;

public final class Client implements Closeable {
  private static Logger log = LogManager.getLogger(Client.class);
  final AsyncHttpClientConfig config;
  AsyncHttpClient client;

  public Client() {
    config = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(Integer.MAX_VALUE).build();
    client = new DefaultAsyncHttpClient(config);
  }

  public void sendMsg(String url, Serializable evt) throws IOException {
    ByteBuffer data = Serializer.serialize(evt);
    Response r;
    try {
      r=client.preparePost(url)
        .setBody(data.array())
        .execute()
        .get();
      log.info("r: {}", r);
    } catch(Exception e) {
      log.info(e);
    }
  }

  public void close() {
    try {
      client.close();
    } catch(IOException e) {}
  }

  public static void main() {
  }

}
