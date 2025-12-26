package im.redpanda.core.exceptions;

public class PeerProtocolException extends Exception {

    public PeerProtocolException(String additionalInformation) {
        super(additionalInformation);
    }
}
