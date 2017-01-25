package kdata;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import kdata.proto.Schema.*;
import kdata.proto.Schema;
import java.io.*;

public class Main {
  private static Logger log = LogManager.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    Schema.Number num = Schema.Number.newBuilder()
      .setName("acme")
      .setValue(12345)
      .build();
    FileOutputStream fos = new FileOutputStream("somenum");
    fos.write(num.toByteArray());
    fos.close();
  }
}
