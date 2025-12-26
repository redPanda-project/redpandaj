package im.redpanda.flaschenpost;

public abstract class GMContent {

  protected byte TYPE;

  private byte[] content;

  protected abstract void computeContent();

  public abstract GMType getGMType();

  protected void setContent(byte[] content) {
    this.content = content;
  }

  public byte[] getContent() {
    if (this.content == null) {
      computeContent();
    }
    return this.content;
  }
}
