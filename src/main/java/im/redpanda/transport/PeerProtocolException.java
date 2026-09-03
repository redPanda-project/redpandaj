package im.redpanda.transport;

public class PeerProtocolException extends Exception {

  public PeerProtocolException(String additionalInformation) {
    super(additionalInformation);
  }
}
