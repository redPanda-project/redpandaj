package im.redpanda.flaschenpost;

public abstract class GMContent {

    protected byte TYPE;

    protected byte[] content;

    protected abstract void computeContent();

    public abstract GMType getGMType();


    public byte[] getContent() {
        if (content == null) {
            computeContent();
        }
        return content;
    }
}
