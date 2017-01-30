package kdata;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import kdata.proto.Numbers.*;
import kdata.proto.Numbers;
import kdata.proto.Orders.*;
import kdata.proto.Orders;
import java.io.*;
import com.opencsv.CSVReader;
import kdb.Client;
import kdb.KdbException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class Main {
  private static Logger log = LogManager.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    /*Numbers.Number num = Numbers.Number.newBuilder()
      .setName("acme")
      .setValue(12345)
      .build();
      FileOutputStream fos = new FileOutputStream("somenum");
      fos.write(num.toByteArray());
      fos.close();*/
    loadOrders(args[0]);
    System.exit(0);
  }

  private static long str2long(String data) {
    long num = -1;
    try {
      num = Long.parseLong(data.trim());
    } catch(Exception e) {}
    return num;
  }

  private static int str2int(String data) {
    int num = -1;
    try {
      num = Integer.parseInt(data.trim());
    } catch(Exception e) {}
    return num;
  }

  private static double str2double(String data) {
    double num = -1;
    try {
      num = Double.parseDouble(data.trim());
    } catch(Exception e) {}
    return num;
  }

  private static void loadOrders(String file) {
    CSVReader reader = null;
    try {
      reader = new CSVReader(new FileReader(file));
      String[] line;
      try (Client client = new Client("http://localhost:8000/", "orders")) {
        client.open();
        List<byte[]> keys = new ArrayList<byte[]>();
        List<byte[]> values = new ArrayList<byte[]>();
        int count = 0;
        while ((line = reader.readNext()) != null) {
          long orderKey = str2long(line[0]);
          Orders.Order order = Orders.Order.newBuilder()
            .setOrderkey(orderKey)
            .setCustkey(str2long(line[1]))
            .setOrderstatus(line[2])
            .setTotalprice(str2double(line[3]))
            .setOrderdate(line[4])
            .setOrderpriority(line[5])
            .setClerk(line[6])
            .setShippriority(str2int(line[7]))
            .setComment(line[8])
            .build();
          keys.add(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(orderKey).array());
          values.add(order.toByteArray());
          count++;
          if(count >= 100) {
            client.put(keys, values);
            keys.clear();
            values.clear();
            count = 0;
          }
        }
        if(count > 0) {
          client.put(keys, values);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
