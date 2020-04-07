package im.redpanda.flaschenpost;

public class GMClientMessage extends GMContent {


    @Override
    protected  void computeContent() {
    }

    @Override
    public GMType getGMType() {
        return GMType.CLIENT_MESSAGE;
    }
}
