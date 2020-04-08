package im.redpanda.flaschenpost;

public enum GMType {
    GARLIC_MESSAGE(1),
    CLIENT_MESSAGE(2),
    ECHO(7),
    TEST(6);


    private final byte id;

    GMType(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return this.id;
    }


}
