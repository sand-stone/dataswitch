package ktsdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.common.collect.ImmutableList;

import java.util.List;
import javax.ws.rs.DefaultValue;
import io.netty.buffer.ByteBuf;
import org.restexpress.RestExpress;
import org.restexpress.Request;
import org.restexpress.Response;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class SuggestResource {
  private static Logger log = LogManager.getLogger(SuggestResource.class);

  private final Kdb ts;

  public SuggestResource(Kdb ts) {
    this.ts = ts;
  }

  public ByteBuf create(Request request, Response response) {
    response.setResponseCreated();
    return request.getBody();
  }

  public String read(Request request, Response response) {
    log.info("request {}", request);
    JsonNode body = null;
    ByteBuf buffer = request.getBody();
    byte[] buf = new byte[buffer.readableBytes()];
    buffer.readBytes(buf);
    //JsonNode body = mapper.readTree(buf);
    response.setContentType("application-json");
    response.setBody("json");
    response.setResponseCode(200);
    return "";
  }

  public List<String> get(String type,
                          String query,
                          int max) throws Exception {
    log.trace("suggest; type: {}, query: {}, max: {}", type, query, max);

    if (type.equals("metrics")) {

    } else if (type.equals("tagk")) {

    } else if (type.equals("tagv")) {

    }

    return ImmutableList.of();
  }

  public List<String> post(SuggestRequest request) throws Exception {
    return get(request.type, request.query, request.max);
  }


  private static class SuggestRequest {

    private final String type;

    private final String query;

    @DefaultValue("25")
    private final int max;

    @JsonCreator
    public SuggestRequest(@JsonProperty("type") String type,
                          @JsonProperty("q") String query,
                          @JsonProperty("max") int max) {
      this.type = type;
      this.query = query;
      this.max = max;
    }
  }
}
