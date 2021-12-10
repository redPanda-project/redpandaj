package im.redpanda.core.exceptions;

public class PeerProtocolException extends Exception {

    private String additionalInformation;

    public PeerProtocolException(String additionalInformation) {
        this.additionalInformation = additionalInformation;
    }
}
