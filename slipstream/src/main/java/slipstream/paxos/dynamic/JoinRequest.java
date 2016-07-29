package slipstream.paxos.dynamic;

import slipstream.paxos.communication.Member;

import java.io.Serializable;

public class JoinRequest implements Serializable {
  public final Member joiner;

  public JoinRequest(Member joiner) {
    this.joiner = joiner;
  }
}
