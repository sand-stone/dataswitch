package slipstream;

import java.io.Serializable;

public class ShardMessage {
  String shard;
  String[] uris;
  
  public ShardMessage(String shard, String[] uris) {
    this.shard = shard;
    this.uris = uris;
  }

  public String getShard() {
    return shard; 
  }

  public String[] getUris() {
    return uris;
  }
  
}
