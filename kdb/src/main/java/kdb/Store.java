package kdb;

import java.nio.*;
import java.io.*;
import java.lang.reflect.Array;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import static java.util.stream.Collectors.toList;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.*;
import kdb.proto.Database.*;
import kdb.proto.Database.Message.MessageType;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class Store implements Closeable {
  private static Logger log = LogManager.getLogger(Store.class);
  private String db;
  private String location;
  private ConcurrentHashMap<String, DataTable> tables;
  private Timer timer;

  static {
    RocksDB.loadLibrary();
  }

  static class Cursor {
    public RocksIterator cursor;
    public byte[] marker;

    public Cursor(RocksIterator cursor, byte[] marker) {
      this.cursor = cursor;
      this.marker = marker;
    }

    public void close() {
      cursor.close();
    }
  }

  static class DataTable {
    public RocksDB db;
    public List<ColumnFamilyDescriptor> colDs;
    public LinkedHashMap<String, ColumnFamilyHandle> columns;
    public String merge;
    public ConcurrentHashMap<String, Cursor> cursors;
    public Statistics stats;

    public DataTable() {
      db = null;
      columns = null;
      merge = null;
      colDs = null;
      cursors = new ConcurrentHashMap<String, Cursor>();
      stats = null;
    }

    public ColumnFamilyHandle getCol(String col) {
      if(col.length() == 0)
        return columns.get("default");
      ColumnFamilyHandle handle = columns.get(col);
      if(handle == null) {
        throw new KdbException("col does not exist" + col);
      }
      return handle;
    }

    public void close() {
      cursors.values().stream().forEach(c -> c.close());
      if(columns != null) {
        columns.values().stream().forEach(h -> h.close());
      }
      db.close();
    }
  }

  public Store(String location) {
    Utils.mkdir(location);
    this.location = location;
    tables = new ConcurrentHashMap<String, DataTable>();
    timer = new Timer();
  }

  private static TimerTask wrap(Runnable r) {
    return new TimerTask() {
      @Override
      public void run() {
        r.run();
      }
    };
  }

  private RocksDB getDB(Options options, String path, int ttl) throws RocksDBException {
    if(ttl == -1) {
      return RocksDB.open(options, path);
    }
    //log.info("create {} ttl {}", path, ttl);
    return TtlDB.open(options, path, ttl, false);
  }

  private RocksDB getDB(final DBOptions options, final String path,
                       final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                       final List<ColumnFamilyHandle> columnFamilyHandles,
                       int ttl) throws RocksDBException {

    if(ttl == -1) {
      return RocksDB.open(options, path, columnFamilyDescriptors, columnFamilyHandles);
    } else {
      List<Integer> ttls = new ArrayList<Integer>();
      for(int i = 0; i < columnFamilyHandles.size(); i++) {
        ttls.add(ttl);
      }
      return TtlDB.open(options, path, columnFamilyDescriptors, columnFamilyHandles, ttls, false);
    }
  }

  private CompressionType getCompression(String name) {
    CompressionType ct = CompressionType.NO_COMPRESSION;
    switch(name) {
    case "snappy":
      ct = CompressionType.SNAPPY_COMPRESSION;
      break;
    case "lz4":
      ct = CompressionType.LZ4_COMPRESSION;
      break;
    case "z":
      ct = CompressionType.ZLIB_COMPRESSION;
      break;
    case "lz4hc":
      ct = CompressionType.LZ4HC_COMPRESSION;
      break;
    case "bzip2":
      ct = CompressionType.BZLIB2_COMPRESSION;
      break;
    }
    return ct;
  }

  public synchronized Message open(OpenOperation op) {
    String table = op.getTable();
    if(table == null || table.length() == 0)
      return MessageBuilder.buildResponse("table name needed");

    if(tables.get(table) == null) {
      String path = location+"/"+table;
      Utils.mkdir(path);
      DataTable dt = new DataTable();
      String mergeOperator = op.getMergeOperator();
      int ttl = op.getTtl();
      //log.info("create {} ttl {}", table, ttl);
      try(Options options = new Options().setCreateIfMissing(true)) {
        //options.createStatistics();
        options.setCompressionType(getCompression(op.getCompression()));
        options.setAllowConcurrentMemtableWrite(true);
        options.setEnableWriteThreadAdaptiveYield(true);
        options.setMaxBackgroundCompactions(Runtime.getRuntime().availableProcessors()*2);
        options.setMaxBackgroundFlushes(Runtime.getRuntime().availableProcessors());
        //dt.stats = options.statisticsPtr();
        //timer.schedule(wrap(() -> log.info(dt.stats.toString())), 10000);
        //options.setStatsDumpPeriodSec(10);
        //log.info("{} merge: <{}>", op, mergeOperator);
        if(mergeOperator != null && mergeOperator.length() > 0) {
          if(mergeOperator.equals("add")) {
            dt.merge = mergeOperator;
            options.setMergeOperatorName("uint64add");
          } else if(mergeOperator.equals("append")) {
            dt.merge = mergeOperator;
            options.setMergeOperatorName("stringappend");
          }  else if(mergeOperator.equals("max")) {
            dt.merge = mergeOperator;
            options.setMergeOperatorName("max");
          } else {
            return MessageBuilder.buildResponse("wrong merge operator, valid ones: add, append, max");
          }
        }
        RocksDB db = null;
        try {
          List<String> columns = op.getColumnsList();
          if(columns.size() == 0) {
            db = getDB(options, path, ttl);
          } else {
            try(final RocksDB db2 = getDB(options, path, ttl)) {
              assert(db2 != null);
              columns.stream().forEach(col -> {
                  try(ColumnFamilyHandle columnFamilyHandle = db2
                      .createColumnFamily(
                                          new ColumnFamilyDescriptor(col.getBytes(),
                                                                     new ColumnFamilyOptions()))) {
                  } catch (RocksDBException e) {
                    log.info(e);
                  }
                });
            } catch (RocksDBException e) {
              log.info(e);
              return MessageBuilder.buildResponse("open: " + e.getMessage());
            }
            dt.colDs = new ArrayList<>();
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            dt.colDs
              .add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
                                              new ColumnFamilyOptions()));
            columns.stream().forEach(col -> {
                dt.colDs
                  .add(new ColumnFamilyDescriptor(col.getBytes(),
                                                  new ColumnFamilyOptions()));
              });
            try(DBOptions dboptions = new DBOptions()) {
              db = getDB(dboptions, path, dt.colDs, handles, ttl);
            }
            dt.columns = new LinkedHashMap<String, ColumnFamilyHandle>();
            //log.info("handles {} columns {}", handles.size(), columns);
            dt.columns.put("default", handles.get(0));
            for(int i = 0; i < columns.size(); i++) {
              dt.columns.put(columns.get(i), handles.get(i+1));
            }
          }
        } catch (RocksDBException e) {
          log.info(e);
        }
        dt.db = db;
        tables.putIfAbsent(table, dt);
      }
    }
    return MessageBuilder.buildResponse("open " + table);
  }

  public Message compact(CompactOperation op) {
    //log.info("compact {}", op);
    DataTable table = tables.get(op.getTable());
    if(table == null)
      return MessageBuilder.buildErrorResponse("table name needed");

    if(op.getColumn() != null && op.getColumn().length() > 0) {
      ColumnFamilyHandle handle = table.columns.get(op.getColumn());
      if(handle == null) {
        return MessageBuilder.buildErrorResponse("column does not exist");
      }
      if(op.getBegin().size() == 0 || op.getEnd().size() == 0) {
        try {
          table.db.compactRange(handle);
        } catch(RocksDBException e) {
          log.info(e);
        }
      } else {
        try {
          table.db.compactRange(handle, op.getBegin().toByteArray(), op.getEnd().toByteArray());
        } catch(RocksDBException e) {
          log.info(e);
        }
      }
    } else {
      if(op.getBegin().size() == 0 || op.getEnd().size() == 0) {
        try {
          table.db.compactRange();
        } catch(RocksDBException e) {
          log.info(e);
        }
      } else {
        try {
          table.db.compactRange(op.getBegin().toByteArray(), op.getEnd().toByteArray());
        } catch(RocksDBException e) {
          log.info(e);
        }
      }
    }
    return MessageBuilder.buildResponse("compact " + table);
  }

  public Message drop(DropOperation op) {
    //log.info("drop {}", op);
    String table = op.getTable();
    if(table == null || table.length() == 0)
      return MessageBuilder.buildErrorResponse("table name needed");

    String path = location+"/"+table;
    if(tables.get(table) == null) {
      if(!Utils.checkFile(path))
        return MessageBuilder.buildErrorResponse("table does not exist:" + table);
      Utils.deleteFile(path);
      return MessageBuilder.buildResponse("drop " + table);
    }

    String col = op.getColumn();
    if(col != null && col.length() > 0) {
      DataTable t = tables.get(table);
      try {
        //log.info("drop col {}", col);
        t.db.dropColumnFamily(t.getCol(col));
      } catch(RocksDBException e) {
        log.info(e);
        return MessageBuilder.buildResponse("cannot drop " + col);
      } catch(KdbException e) {
        log.info(e);
        return MessageBuilder.buildResponse("cannot drop " + col);
      }
      return MessageBuilder.buildResponse("drop " + col);
    }

    tables.remove(table).close();
    //log.info("delete {}", path);
    Utils.deleteFile(path);
    return MessageBuilder.buildResponse("drop " + table);
  }

  public Message update(PutOperation op) {
    String name = op.getTable();
    DataTable table = tables.get(name);
    if(table == null) {
      return MessageBuilder.buildErrorResponse("table not opened:" + table);
    }

    int len = op.getKeysCount();
    if(len != op.getValuesCount()) {
      return MessageBuilder.buildErrorResponse("data length wrong");
    }

    if(op.getColumn() != null && op.getColumn().length() > 0) {
      //log.info("op.getColumn() {}", op.getColumn());
      ColumnFamilyHandle handle = table.columns.get(op.getColumn());
      if(handle == null) {
        return MessageBuilder.buildErrorResponse("column does not exist");
      }
      if(table.merge == null) {
        try(WriteOptions writeOpts = new WriteOptions();
            WriteBatch writeBatch = new WriteBatch()) {
          for(int i = 0; i < len; i++) {
            writeBatch.put(handle, op.getKeys(i).toByteArray(), op.getValues(i).toByteArray());
          }
          table.db.write(writeOpts, writeBatch);
        } catch (Exception e) {
          e.printStackTrace();
          log.info(e);
          return MessageBuilder.buildErrorResponse("updated wrong" + e.getMessage());
        }
      } else {
        //log.info("op.getColumn() {}", op.getColumn());
        try(WriteOptions writeOpts = new WriteOptions();
            WriteBatch writeBatch = new WriteBatch()) {
          for(int i = 0; i < len; i++) {
            writeBatch.merge(handle, op.getKeys(i).toByteArray(), op.getValues(i).toByteArray());
          }
          table.db.write(writeOpts, writeBatch);
        } catch (Exception e) {
          e.printStackTrace();
          log.info(e);
          return MessageBuilder.buildErrorResponse("updated wrong" + e.getMessage());
        }
      }
    } else {
      if(table.merge == null) {
        try(WriteOptions writeOpts = new WriteOptions();
            WriteBatch writeBatch = new WriteBatch()) {
          for(int i = 0; i < len; i++) {
            writeBatch.put(op.getKeys(i).toByteArray(), op.getValues(i).toByteArray());
          }
          table.db.write(writeOpts, writeBatch);
        } catch (Exception e) {
          e.printStackTrace();
          log.info(e);
          return MessageBuilder.buildErrorResponse("updated wrong" + e.getMessage());
        }
      } else {
        try(WriteOptions writeOpts = new WriteOptions();
            WriteBatch writeBatch = new WriteBatch()) {
          for(int i = 0; i < len; i++) {
            writeBatch.merge(op.getKeys(i).toByteArray(), op.getValues(i).toByteArray());
          }
          table.db.write(writeOpts, writeBatch);
        } catch (Exception e) {
          e.printStackTrace();
          log.info(e);
          return MessageBuilder.buildErrorResponse("updated wrong" + e.getMessage());
        }
      }
    }
    return MessageBuilder.buildResponse("updated " + table);
  }

  private ReadOptions getReadOptions() {
    return getReadOptions(false);
  }

  private ReadOptions getReadOptions(boolean prefix) {
    ReadOptions readOptions = new ReadOptions();
    readOptions.setTotalOrderSeek(true);
    if(prefix)
      readOptions.setPrefixSameAsStart(prefix);
    return readOptions;
  }

  private void walk(Cursor cursor, ScanOperation.Type dir, int limit, List<byte[]> keys, List<byte[]> values) {
    int count = 0;
    byte[] marker = cursor.marker;
    if(marker == null) {
      switch(dir) {
      case Next:
        while(cursor.cursor.isValid()) {
          byte[] key = cursor.cursor.key();
          byte[] value = cursor.cursor.value();
          keys.add(Arrays.copyOf(key, key.length));
          values.add(Arrays.copyOf(value, value.length));
          cursor.cursor.next();
          if(++count >= limit)
            return;
        }
        break;
      case Prev:
        while(cursor.cursor.isValid()) {
          byte[] key = cursor.cursor.key();
          byte[] value = cursor.cursor.value();
          keys.add(Arrays.copyOf(key, key.length));
          values.add(Arrays.copyOf(value, value.length));
          cursor.cursor.prev();
          if(++count >= limit)
            return;
        }
        break;
      }
    } else {
      switch(dir) {
      case Next:
        //log.info("marker {} limit {} count {} ", new String(marker), limit, count);
        while(cursor.cursor.isValid()) {
          byte[] key = cursor.cursor.key();
          byte[] value = cursor.cursor.value();
          if(Utils.memcmp(key, marker, marker.length) < 0) {
            keys.add(Arrays.copyOf(key, key.length));
            values.add(Arrays.copyOf(value, value.length));
            if(++count >= limit) {
              cursor.cursor.next();
              return;
            }
          } else {
            cursor.cursor.next();
            break;
          }
          cursor.cursor.next();
        }
        //log.info("seek next {}", count);
        break;
      case Prev:
        while(cursor.cursor.isValid()) {
          byte[] key = cursor.cursor.key();
          byte[] value = cursor.cursor.value();
          if(Utils.memcmp(key, marker, marker.length) > 0) {
            keys.add(Arrays.copyOf(key, key.length));
            values.add(Arrays.copyOf(value, value.length));
            if(++count >= limit) {
              cursor.cursor.prev();
              return;
            }
          } else {
            cursor.cursor.next();
            break;
          }
          cursor.cursor.prev();
        }
        break;
      }
    }
  }

  public Message get(GetOperation op) {
    String name = op.getTable();
    DataTable table = tables.get(name);
    if(table != null) {
      if(op.getKeysCount() > 1000) {
        //review: random guess
        return MessageBuilder.buildErrorResponse("batch size too big");
      }
      String col = op.getColumn();
      if(col != null && col.length() > 0) {
        try {
          int count = op.getKeysCount();
          List<ColumnFamilyHandle> handles = new ArrayList<ColumnFamilyHandle>(count);
          ColumnFamilyHandle handle = table.getCol(col);
          if(handle == null)
            return MessageBuilder.buildErrorResponse("wrong column:" + col);
          for(int i = 0; i < count; i++)
            handles.add(handle);
          log.info("col <{}> handle {}", col, handle);
          return MessageBuilder.buildResponse(table
                                              .db
                                              .multiGet(handles,
                                                        op
                                                        .getKeysList()
                                                        .stream()
                                                        .map(k -> k.toByteArray())
                                                        .collect(toList())));
        } catch(RocksDBException e) {
          log.info(e);
          return MessageBuilder.buildErrorResponse("table get errr:" + e.getMessage());
        }
      } else {
        try {
          return MessageBuilder.buildResponse(table
                                              .db
                                              .multiGet(op
                                                        .getKeysList()
                                                        .stream()
                                                        .map(k -> k.toByteArray())
                                                        .collect(toList())));
        } catch(RocksDBException e) {
          log.info(e);
          return MessageBuilder.buildErrorResponse("table get errr:" + e.getMessage());
        }
      }
    }
    return MessageBuilder.buildErrorResponse("table not opened:" + table);
  }

  private  boolean isempty(String col) {
    return col == null || col.length() == 0;
  }

  public Message scan(ScanOperation op) {
    //log.info("scan {}", op);
    String name = op.getTable();
    DataTable table = tables.get(name);
    if(table == null) {
      return MessageBuilder.buildErrorResponse("table not opened:" + table);
    }
    RocksIterator iter = null;
    Cursor cursor = null;
    String token = "";
    String col = op.getColumn();
    int limit = op.getLimit();
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    switch(op.getOp()) {
    case First:
      if(isempty(col))
        iter = table.db.newIterator(getReadOptions());
      else
        iter = table.db.newIterator(table.getCol(col), getReadOptions());
      iter.seekToFirst();
      cursor = new Cursor(iter, null);
      token = cursor.toString();
      table.cursors.put(token, cursor);
      walk(cursor, ScanOperation.Type.Next, limit, keys, values);
      break;
    case Last:
      if(isempty(col))
        iter = table.db.newIterator(getReadOptions());
      else
        iter = table.db.newIterator(table.getCol(col), getReadOptions());
      iter.seekToLast();
      cursor = new Cursor(iter, null);
      token = cursor.toString();
      table.cursors.put(token, cursor);
      walk(cursor, ScanOperation.Type.Prev, limit, keys, values);
      break;
    case Close:
      token = op.getToken();
      cursor = table.cursors.remove(token);
      //log.info("close token <{}> ==> {}", token, cursor);
      if(cursor != null) {
        cursor.close();
        token = "";
      }
      break;
    case ScanNext:
      //log.info("scan col: {}", col);
      if(isempty(col))
        iter = table.db.newIterator(getReadOptions());
      else
        iter = table.db.newIterator(table.getCol(col), getReadOptions());
      iter.seek(op.getKey().toByteArray());
      cursor = new Cursor(iter, op.getKey2().toByteArray());
      token = cursor.toString();
      table.cursors.put(token, cursor);
      walk(cursor, ScanOperation.Type.Next, limit, keys, values);
      break;
    case ScanPrev:
      if(isempty(col))
        iter = table.db.newIterator(getReadOptions());
      else
        iter = table.db.newIterator(table.getCol(col), getReadOptions());
      iter.seek(op.getKey().toByteArray());
      cursor = new Cursor(iter, op.getKey2().toByteArray());
      token = cursor.toString();
      table.cursors.put(token, cursor);
      walk(cursor, ScanOperation.Type.Prev, limit, keys, values);
      break;
    case Next:
      token = op.getToken();
      cursor = table.cursors.get(token);
      if(cursor != null) {
        walk(cursor, ScanOperation.Type.Next, limit, keys, values);
        if(keys.size() == 0)
          token = "";
      }
      break;
    case Prev:
      token = op.getToken();
      cursor = table.cursors.get(token);
      if(cursor != null) {
        walk(cursor, ScanOperation.Type.Prev, limit, keys, values);
        if(keys.size() == 0)
          token = "";
      }
      break;
    }
    return  MessageBuilder.buildResponse(token, keys, values);
  }

  public Message handle(ByteBuffer data) throws IOException {
    byte[] arr = new byte[data.remaining()];
    data.get(arr);
    Message msg = Message.parseFrom(arr);
    //log.info("handle {}", msg);
    if(msg.getType() == MessageType.Put) {
      PutOperation op = msg.getPutOp();
      msg = update(op);
    } else if(msg.getType() == MessageType.Open) {
      OpenOperation op = msg.getOpenOp();
      msg = open(op);
    } else if(msg.getType() == MessageType.Drop) {
      msg = drop(msg.getDropOp());
    } else if(msg.getType() == MessageType.Compact) {
      msg = compact(msg.getCompactOp());
    }
    return msg;
  }

  public void close() { }

}