package slipstream.paxos.messages;

import slipstream.paxos.communication.Member;

import java.io.Serializable;

public interface MessageWithSender extends Serializable {
  Member getSender();
}
