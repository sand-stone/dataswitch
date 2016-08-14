package slipstream;

import java.io.Serializable;
import slipstream.paxos.messages.MessageWithSender;
import slipstream.paxos.communication.Member;

public class ShardMessage implements MessageWithSender {
  String shard;
  String[] uris;
  Member sender;
  
  public ShardMessage(Member sender, String shard, String[] uris) {
    this.sender = sender;
    this.shard = shard;
    this.uris = uris;
  }

  public Member getSender() {
    return sender; 
  }

  public String getShard() {
    return shard; 
  }

  public String[] getUris() {
    return uris;
  }
  
}
