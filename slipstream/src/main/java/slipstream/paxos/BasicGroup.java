package slipstream.paxos;

import slipstream.paxos.communication.CommLayer;
import slipstream.paxos.communication.UDPMessenger;

import java.io.Serializable;
import java.net.SocketException;
import java.net.UnknownHostException;

/**
 * This is the basic totally ordered reliable broadcast implementation. It has static membership and it doesn't
 * fragment messages. If the underlying communication layer doesn't fragment messages either, it
 * will keep on failing to transmit. In order to support larger messages you can use the
 * {@link paxos.fragmentation.FragmentingMessenger} along with this class.
 * {@link paxos.fragmentation.FragmentingGroup} might be a better choice because it should deal better
 * with unreliable communication.
 *
 * This class does not persist state, thus it doesn't support recovery of members.
 *
 * @see paxos.dynamic.DynamicGroup
 * @see paxos.fragmentation.FragmentingGroup
 **/
public class BasicGroup implements CommLayer.MessageListener {
  private final AcceptorLogic acceptorLogic;
  private final LeaderLogic leaderLogic;
  private final FailureDetector failureDetector;
  private final CommLayer commLayer;

  public BasicGroup(GroupMembership membership, Receiver receiver) throws SocketException, UnknownHostException {
    this(membership, new UDPMessenger(membership.getUID().getPort()), receiver);
  }

  public BasicGroup(GroupMembership membership, CommLayer commLayer, Receiver receiver) {
    this(membership, commLayer, receiver, System.currentTimeMillis());
  }

  public BasicGroup(GroupMembership membership, CommLayer commLayer, Receiver receiver, long time) {
    this.commLayer = commLayer;

    leaderLogic = new LeaderLogic(membership, commLayer, time);
    acceptorLogic = new AcceptorLogic(membership, commLayer, receiver);
    failureDetector = new FailureDetector(membership, commLayer, leaderLogic);

    this.commLayer.setListener(this);
  }

  public void broadcast(Serializable message) {
    acceptorLogic.broadcast(message);
  }

  public void close() {
    commLayer.close();
  }

  private void dispatch(Serializable message) {
    leaderLogic.dispatch(message);
    acceptorLogic.dispatch(message);
    failureDetector.dispatch(message);
  }

  public void receive(byte[] message) {
    dispatch((Serializable) PaxosUtils.deserialize(message));
  }
}
