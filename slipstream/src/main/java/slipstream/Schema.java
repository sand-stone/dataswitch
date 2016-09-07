package slipstream;

import java.util.*;
import com.google.common.hash.*;

public class Schema {

  public final static byte dbtype_none = (byte)0;
  public final static byte dbtype_mysql = (byte)1;
  public final static byte dbtype_oracle = (byte)2;

  public static class DataSource {
    public String uri;
    public byte dbtype;
    public long ts;
    public String auth;
  }

  public static class Ensemble {
    public String name;
    public List<String> nodes;
  }

  public static class Shard {
    public String uri;
    public List<String> ensemable;
  }

}
