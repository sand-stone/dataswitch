package dstream;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public final class Message implements Serializable {
  private static Logger log = LogManager.getLogger(Message.class);

  public static final class CreateTable implements Serializable {
    public Table table;

    public CreateTable(Table table) {
      this.table = table;
    }
  }

   public static final class UpsertTable implements Serializable {
     public String table;
     public List<String> names;
     public List<Object> values;

     public UpsertTable(String table, List<String> names, List<Object> values) {
       this.table = table;
       this.names = names;
       this.values = values;
     }
  }

  public static final class DeleteTable implements Serializable {
    public String table;

    public DeleteTable(String table) {
      this.table = table;
    }
  }

  public static final class QueryTable implements Serializable {
    public String table;

    public QueryTable(String table) {
      this.table = table;
    }
  }

}
