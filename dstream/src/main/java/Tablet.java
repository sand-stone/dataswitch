package dstream;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Tablet {
  private static Logger log = LogManager.getLogger(Tablet.class);

  private String db;
  private final  String dbconfig = "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3), checkpoint=(log_size=2GB,wait=3600)";

  //"type=lsm,key_format=qSS,value_format=u,"+"columns=(ts,host,metric,val)";
  private String getStorge() {
    return null;
  }

  public Tablet(String location) {

  }
}
