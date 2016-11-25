package kdb;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import java.util.concurrent.Future;
import static java.util.stream.Collectors.*;
import java.util.stream.Collectors;

public class KdbIntegrationTest extends TestCase {
  private static Logger log = LogManager.getLogger(KdbIntegrationTest.class);

  public KdbIntegrationTest(String testName) {
    super(testName);
  }

  public static Test suite()  {
    return new TestSuite(KdbIntegrationTest.class);
  }

  public void test1() {
    String table = "test1";
    int c = 5;
    Client.Result rsp;
    while (c-->0) {
      try(Client client = new Client("http://localhost:8000/", table)) {
        client.open();
        client.drop();
      }
    }
    assertTrue(true);
  }

  public void test2() {
    String table = "test2";
    try(Client client = new Client("http://localhost:8000/", table)) {
      Client.Result rsp = client.open();
      //System.out.println("rsp:" + rsp);
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int count = 10;
      for(int i = 0; i < count; i++) {
        keys.add(("keys"+i).getBytes());
        values.add(("values"+i).getBytes());
      }
      rsp = client.put(keys, values);
      //System.out.println("rsp:" + rsp);
      rsp = client.get(keys);
      Set<String>  r = rsp.keys().stream().map(e -> new String(e)).collect(Collectors.toSet());
      if(r.size() == count)
        assertTrue(true);
      else
        assertTrue(false);
    }
  }

  public void test3() {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "key2".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "val2".getBytes());
    String table = "test3";
    try(Client client = new Client("http://localhost:8000/", table)) {
      client.open();
      client.put(keys, values);
      Client.Result rsp = client.get(keys);
      //log.info("rsp: {}", rsp);
      if(rsp.count() == 2 && (new String(rsp.getValue(0)).equals("val2")
                              || new String(rsp.getValue(1)).equals("val2")
                              )) {
        assertTrue(true);
      } else
        assertTrue(false);
    }
  }

  public void test4() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("test4key"+i).getBytes());
      values.add(("test4value"+i).getBytes());
    }
    String table = "test4";

    try (Client client = new Client("http://localhost:8000/", table)) {
      client.open();
      client.put(keys, values);
      Client.Result rsp = client.scanForward("test4key2".getBytes(), "test4key5".getBytes(), 5);
      //log.info("rsp {}", rsp);
      if(rsp.count() == 3)
        assertTrue(true);
      else
        assertTrue(false);
      client.drop();
    }
  }

  public static byte[] longToByte(long value) {
    ByteBuffer longBuffer = ByteBuffer.allocate(8)
      .order(ByteOrder.nativeOrder());
    longBuffer.clear();
    longBuffer.putLong(value);
    return longBuffer.array();
  }

  public static long byteToLong(byte[] data)  {
    ByteBuffer longBuffer = ByteBuffer.allocate(8)
      .order(ByteOrder.nativeOrder());
    longBuffer.put(data, 0, 8);
    longBuffer.flip();
    return longBuffer.getLong();
  }

  public void test5() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("key"+i).getBytes());
      values.add(longToByte(i));
    }

    String table = "test5";
    try(Client client = new Client("http://localhost:8000/", table)) {
      client.open("add");
      client.put(keys, values);
      Client.Result rsp = client.scanForward("key2".getBytes(), "key5".getBytes(), 5);
      long s1 = rsp.values().stream().map(e -> (int)byteToLong(e))
        .collect(Collectors.toList())
        .stream()
        .reduce(0, Integer::sum);
      client.put(keys, values);
      rsp = client.scanForward("key2".getBytes(), "key5".getBytes(), 5);
      long s2 = rsp.values().stream().map(e -> (int)byteToLong(e))
        .collect(Collectors.toList())
        .stream()
        .reduce(0, Integer::sum);
      if(s1*2 == s2) {
        assertTrue(true);
      } else {
        assertTrue(false);
      }
    }
  }


  public void test6() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("key"+i).getBytes());
      values.add("value".getBytes());
    }

    String table = "test6";
    try(Client client = new Client("http://localhost:8000/", table)) {
      client.open("append");
      client.put(keys, values);
      Client.Result rsp = client.scanForward("key2".getBytes(), "key5".getBytes(), 5);
      long s1 = rsp.values().stream().map(e -> e.length)
        .collect(Collectors.toList())
        .stream()
        .reduce(0, Integer::sum);
      client.put(keys, values);
      rsp = client.scanForward("key2".getBytes(), "key5".getBytes(), 5);
      long s2 = rsp.values().stream().map(e -> e.length)
        .collect(Collectors.toList())
        .stream()
        .reduce(0, Integer::sum);
      //log.info("s1 {} s2 {}", s1, s2);
      if(s1*2 + 3 == s2) {
        assertTrue(true);
      } else {
        assertTrue(false);
      }
    }
  }

  public void test7() {
    int c = 2;

    while(c-->0) {
      String table = "test7";
      try (Client client = new Client("http://localhost:8001/", table)) {
        client.open();
        int count = 10;
        List<byte[]> keys = new ArrayList<byte[]>();
        List<byte[]> values = new ArrayList<byte[]>();

        UUID guid1 = UUID.randomUUID();
        UUID guid2 = UUID.randomUUID();

        for (int i = 0; i < count; i++) {
          ByteBuffer key = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
          key.put((byte)0);
          key.putLong(guid1.getMostSignificantBits()).putLong(guid1.getLeastSignificantBits());
          key.put((byte)i);
          keys.add(key.array());
          values.add(("value"+i).getBytes());
        }
        client.put(keys, values);

        keys.clear();
        values.clear();

        for (int i = 0; i < count; i++) {
          ByteBuffer key = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
          key.put((byte)1);
          key.putLong(guid2.getMostSignificantBits()).putLong(guid2.getLeastSignificantBits());
          key.put((byte)i);
          keys.add(key.array());
          values.add(("value"+i).getBytes());
        }
        client.put(keys, values);

        keys.clear();
        values.clear();

        Client.Result rsp = client.scanForward(new byte[]{0}, new byte[]{1}, 100);
        //log.info("test7 rsp {}", rsp);
        assertTrue(rsp.count() == 10);

        ByteBuffer key1 = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
        key1.put((byte)1);
        key1.putLong(guid2.getMostSignificantBits()).putLong(guid2.getLeastSignificantBits());
        key1.put((byte)0);
        ByteBuffer key2 = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
        key2.put((byte)1);
        key2.putLong(guid2.getMostSignificantBits()).putLong(guid2.getLeastSignificantBits());
        key2.put((byte)10);
        rsp = client.scanForward(key1.array(), key2.array(), 100);
        //log.info("msg {} ==> {} ", rsp, rsp.count());
        assertTrue(rsp.count() == 10);
        client.drop();
      }
    }
  }

  public void test8() {
    String table = "test8";
    try(Client client = new Client("http://localhost:8000/", table)) {
      List<String> cols = Arrays.asList("test8col1", "test8col2");
      Client.Result rsp = client.open(cols);
      //System.out.println("rsp:" + rsp);
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int count = 10;
      for(int i = 0; i < count; i++) {
        keys.add(("keys"+i).getBytes());
        values.add(("values"+i).getBytes());
      }
      rsp = client.put("test8col2", keys, values);
      //System.out.println("rsp:" + rsp);
      rsp = client.get("test8col2", keys);
      //System.out.println("rsp:" + rsp);
      Set<String>  r = rsp.keys().stream().map(e -> new String(e)).collect(Collectors.toSet());
      if(r.size() == count)
        assertTrue(true);
      else
        assertTrue(false);

      client.drop("test8col2");
    }
  }

  public void test9() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("test9key"+i).getBytes());
      values.add(("value"+i).getBytes());
    }

    String table = "test9";
    List<String> cols = Arrays.asList("test9col1", "test9col2");

    try(Client client = new Client("http://localhost:8000/", table)) {
      client.open(cols);
      client.put("test9col2", keys, values);
      //Client.Result rsp = client.get("test9col2", keys);
      Client.Result rsp = client.scanForward("test9col2", "test9key2".getBytes(), "test9key5".getBytes(), 5);
      //log.info("rsp {}", rsp);
      long s1 = rsp.values().stream().map(e -> e.length)
        .collect(Collectors.toList())
        .stream()
        .reduce(0, Integer::sum);
      List<byte[]> values2 = new ArrayList<byte[]>();
      for (int i = 0; i < rsp.count(); i++) {
        values2.add((new String(rsp.getValue(i))+"data").getBytes());
      }
      client.put("test9col2", rsp.keys(), values2);
      rsp = client.scanForward("test9col2", "test9key2".getBytes(), "test9key5".getBytes(), 5);
      //log.info("rsp {}", rsp);
      long s2 = rsp.values().stream().map(e -> e.length)
        .collect(Collectors.toList())
        .stream()
        .reduce(0, Integer::sum);
      //log.info("s1 {} s2 {}", s1, s2);
      if((s2 -s1) == "data".length()*rsp.count()) {
        assertTrue(true);
      } else {
        assertTrue(false);
      }
      rsp = client.scanForward("test9col2", "test9key2".getBytes(), "test9key5".getBytes(), 1);
      if(rsp.count() == 1)
        assertTrue(true);
      rsp = client.scanNext(2);
      if(rsp.count() == 2)
        assertTrue(true);
    }
  }

  public void test10() {
    String table = "test10";
    try(Client client = new Client("http://localhost:8000/", table)) {
      Client.Result rsp = client.openCompressed("snappy");
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int count = 10;
      for(int i = 0; i < count; i++) {
        keys.add(("keys"+i).getBytes());
        values.add(("values"+i).getBytes());
      }
      rsp = client.put(keys, values);
      //System.out.println("rsp:" + rsp);
      rsp = client.get(keys);
      Set<String>  r = rsp.keys().stream().map(e -> new String(e)).collect(Collectors.toSet());
      if(r.size() == count)
        assertTrue(true);
      else
        assertTrue(false);
    }
  }

}