package im.redpanda.flaschenpost;

public enum GMType {
  /**
   * Garlic message v2 (MS03): AES-256-GCM + X25519 ECDH + HKDF. The type byte doubles as the format
   * version ({@code 0x02}); the pre-MS03 v1 format (type {@code 0x01}, AES-CTR + ECDSA) is no
   * longer parsed.
   */
  GARLIC_MESSAGE(2),
  CLIENT_MESSAGE(3),
  ACK(4),
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
