package kdata;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import kdata.proto.Numbers.*;
import kdata.proto.Numbers;
import java.io.*;

public class Main {
  private static Logger log = LogManager.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    Numbers.Number num = Numbers.Number.newBuilder()
      .setName("acme")
      .setValue(12345)
      .build();
    FileOutputStream fos = new FileOutputStream("somenum");
    fos.write(num.toByteArray());
    fos.close();
  }
}
