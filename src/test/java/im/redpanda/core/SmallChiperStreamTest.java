package im.redpanda.core;

import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import java.security.*;

public class SmallChiperStreamTest {

  public static final String ALGORITHM = "AES/CTR/NoPadding";

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  public static void main(final String[] args) {

    Sha256Hash sha256Hash =
        Sha256Hash.create(
            Utils.parseAsHexOrBase58(
                "0437fb5ab1b9c42505c6fff4fd9a01e8aecf52fd51e3562c5769246587d36a179f95c2748f432c508f10a3a8edf6eb12d2c3367c147892e176c5c4e0bfd2b38c9a"));

    byte[] bytes = sha256Hash.getBytes();

    System.out.println("public key sha256: " + Utils.bytesToHexString(bytes));
  }
}
