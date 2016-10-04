package dstream;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

/** Enumerator that reads from a JSON file. */
class JsonEnumerator implements Enumerator<Object[]> {
  private final Enumerator<Object> enumerator;

  public JsonEnumerator(File file) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
      mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      //noinspection unchecked
      List<Object> list = mapper.readValue(file, List.class);
      enumerator = Linq4j.enumerator(list);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Object[] current() {
    return new Object[] {enumerator.current()};
  }

  public boolean moveNext() {
    return enumerator.moveNext();
  }

  public void reset() {
    enumerator.reset();
  }

  public void close() {
    try {
      enumerator.close();
    } catch (Exception e) {
      throw new RuntimeException("Error closing JSON reader", e);
    }
  }
}
