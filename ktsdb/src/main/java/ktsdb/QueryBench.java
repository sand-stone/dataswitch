package ktsdb;

import java.net.URI;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.google.gson.Gson;

public class QueryBench {
  private static Logger log = LogManager.getLogger(QueryBench.class);
  private Gson gson;

  public static class Configuration {

    Map<String, Object> params;

    public Map<String, Object> getBench() {
      return params;
    }

    public void setBench(Map<String, Object> params) {
      this.params = params;
    }

  }

  public QueryBench() {
    this.gson = new Gson();
  }

  public static void main(String[] args) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      Configuration config = mapper.readValue(new File(args[0]), Configuration.class);
      log.info("config {}", ReflectionToStringBuilder.toString(config, ToStringStyle.MULTI_LINE_STYLE));
      new QueryBench().run(config);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void postQuery(URI url, CloseableHttpClient client, QueryResource.QueryRequest query) {
    HttpPost post = new HttpPost(url);

    try {
      post.setEntity(new StringEntity(
                                      gson.toJson(query),
                                      ContentType.APPLICATION_JSON
                                      ));

      try (CloseableHttpResponse response = client.execute(post)) {
        if (response.getStatusLine().getStatusCode() < 200 ||
            response.getStatusLine().getStatusCode() >= 300) {
          throw new RuntimeException(response.toString());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  QueryResource.QueryRequest buildquery() {
    List<QueryResource.SubQuery> subs = new ArrayList<>();
    Map<String, String> tags = new TreeMap<>();
    tags.put("dc", "dc01");
    subs.add(new QueryResource.SubQuery("metric1", tags, "mean", "down"));
    return new QueryResource.QueryRequest(100000, 20000, subs, true);
  }

  public void run(Configuration config) throws Exception {
    final CloseableHttpClient client = HttpClientBuilder.create().build();

    URI uri = new URIBuilder()
      .setScheme("http")
      .setHost((String)config.getBench().get("ktsdHost"))
      .setPort((int)config.getBench().get("ktsdPort"))
      .setPath("/api/query")
      .build();

    postQuery(uri, client, buildquery());

  }
}
