package im.redpanda.core;

import im.redpanda.crypt.Base58;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;

public class SecureKeyGenerator {

  @Test
  public void generateAndApplyKeys() throws IOException {
    System.out.println("Generating new secure keys...");

    // 1. Generate Keys
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    NodeId nodeId = new NodeId();
    String privateKey = Base58.encode(nodeId.exportWithPrivate());
    String publicKey = Base58.encode(nodeId.exportPublic());

    // 2. Write Private Key to file
    Path privateKeyPath = Path.of("privateSigningKey.txt");
    Files.write(privateKeyPath, privateKey.getBytes(StandardCharsets.UTF_8));
    System.out.println("New private key written to: " + privateKeyPath.toAbsolutePath());

    // 3. Update Updater.java with new Public Key
    Path updaterPath = Path.of("src/main/java/im/redpanda/core/Updater.java");
    if (Files.exists(updaterPath)) {
      String content = new String(Files.readAllBytes(updaterPath), StandardCharsets.UTF_8);

      String regex =
          "(public static final String PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS = \")[^\"]*(\";)";
      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(content);

      if (matcher.find()) {
        String newContent = matcher.replaceFirst("$1" + publicKey + "$2");
        Files.write(updaterPath, newContent.getBytes(StandardCharsets.UTF_8));
        System.out.println("Updater.java successfully updated with new public key: " + publicKey);
      } else {
        System.err.println(
            "Could not find PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS constant in Updater.java to update.");
      }
    } else {
      System.err.println(
          "Updater.java not found at expected path: " + updaterPath.toAbsolutePath());
    }
  }
}
