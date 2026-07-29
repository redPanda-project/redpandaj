package im.redpanda.crypt;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.bouncycastle.util.encoders.Hex;

public class Utils {

  private static final MessageDigest digest;

  static {
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private Utils() {}

  /** See {@link Utils#doubleDigest(byte[], int, int)}. */
  public static byte[] doubleDigest(byte[] input) {
    return doubleDigest(input, 0, input.length);
  }

  /**
   * Calculates the SHA-256 hash of the given byte range, and then hashes the resulting hash again.
   * This is standard procedure in Bitcoin. The resulting hash is in big endian form.
   */
  public static byte[] doubleDigest(byte[] input, int offset, int length) {
    synchronized (digest) {
      digest.reset();
      digest.update(input, offset, length);
      byte[] first = digest.digest();
      return digest.digest(first);
    }
  }

  /** Returns the given byte array hex encoded. */
  public static String bytesToHexString(byte[] bytes) {
    StringBuffer buf = new StringBuffer(bytes.length * 2);
    for (byte b : bytes) {
      String s = Integer.toString(0xFF & b, 16);
      if (s.length() < 2) {
        buf.append('0');
      }
      buf.append(s);
    }
    return buf.toString();
  }

  /**
   * Attempts to parse the given string as arbitrary-length hex or base58 and then return the
   * results, or null if neither parse was successful.
   */
  public static byte[] parseAsHexOrBase58(String data) {
    try {
      return Hex.decode(data);
    } catch (Exception e) {
      // Didn't decode as hex, try base58.
      try {
        return Base58.decodeChecked(data);
      } catch (AddressFormatException e1) {
        return null;
      }
    }
  }

  public static String formatDuration(long millis) {
    return formatDuration(Duration.ofMillis(millis));
  }

  public static String formatDurationFromNow(long millis) {
    return formatDuration(Duration.ofMillis(System.currentTimeMillis() - millis));
  }

  public static String formatDuration(Duration duration) {
    long seconds = duration.getSeconds();
    long absSeconds = Math.abs(seconds);
    String positive =
        "%d:%02d:%02d".formatted(absSeconds / 3600, absSeconds % 3600 / 60, absSeconds % 60);
    return seconds < 0 ? "-" + positive : positive;
  }

  /**
   * Whether the given host is only meaningful inside the network of whoever is using it: loopback,
   * the RFC1918 private ranges, carrier-grade NAT, link-local and the unspecified address, plus the
   * IPv6 equivalents. Such an address must never be handed to a peer outside that network — it
   * either points nowhere or, worse, at something in the receiver's own LAN.
   *
   * <p>Matching is done on the string on purpose: this runs on the peer-list gossip path, where a
   * DNS lookup would block a network thread on input an attacker controls. A host name we cannot
   * classify without resolving it (anything that is not an IP literal) is reported as non-local.
   *
   * <p>A {@code null} or blank host counts as local: it is not usable by anyone else either, and
   * every caller wants the conservative answer for it.
   *
   * <p>Historically this method returned true for all of {@code 192.*}, which also covers public
   * space; only {@code 192.168.0.0/16} is private.
   */
  public static boolean isLocalAddress(String string) {
    String host = normalizeHost(string);
    if (host.isEmpty()) {
      return true;
    }
    if (host.equals("localhost") || host.endsWith(".localhost") || host.equals("localhost.")) {
      return true;
    }
    if (host.startsWith("::ffff:")) {
      // IPv4-mapped IPv6 literal, classify by the embedded IPv4 address
      host = host.substring("::ffff:".length());
    }
    if (host.indexOf(':') >= 0) {
      // IPv6 literal
      if (host.equals("::") || host.equals("::1")) {
        return true; // unspecified / loopback
      }
      if (host.matches("0(:0)*:0*1")) {
        return true; // fully written out loopback, e.g. 0:0:0:0:0:0:0:1
      }
      if (host.matches("0(:0)*")) {
        return true; // fully written out unspecified address
      }
      // fc00::/7 unique local, fe80::/10 link local
      return host.startsWith("fc")
          || host.startsWith("fd")
          || host.startsWith("fe8")
          || host.startsWith("fe9")
          || host.startsWith("fea")
          || host.startsWith("feb");
    }

    int[] octets = parseIpv4(host);
    if (octets == null) {
      // not an IP literal, so a host name — we do not resolve it here, see javadoc
      return false;
    }
    return octets[0] == 0 // "this network", includes 0.0.0.0
        || octets[0] == 127 // loopback
        || octets[0] == 10 // RFC1918
        || (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31) // RFC1918
        || (octets[0] == 192 && octets[1] == 168) // RFC1918
        || (octets[0] == 100 && octets[1] >= 64 && octets[1] <= 127) // RFC6598 CGNAT
        || (octets[0] == 169 && octets[1] == 254); // link local
  }

  /**
   * Whether an {@code ip:port} that a peer advertises to us — or that we are about to advertise to
   * a peer — is plausible for the other side of that exchange, which we reached (or which reached
   * us) at {@code peerIp}.
   *
   * <p>Peer-list gossip is unauthenticated, so without this check any peer can make us dial
   * arbitrary addresses: a list full of {@code 127.0.0.1} or LAN addresses turns every node into a
   * scanner of its own host and network, and the entries spread further because we re-gossip our
   * peer list verbatim.
   *
   * <p>The rule is deliberately about <em>who</em> advertises <em>what</em> rather than a blanket
   * ban on local addresses, because the local test topologies depend on the latter: nodes started
   * on {@code 127.0.0.1} by the mobile e2e suite must keep discovering each other, and the emulator
   * gate reaches the host at {@code 10.0.2.2}. A peer we talk to over a local address may therefore
   * gossip local addresses; a peer we talk to over a public address may not.
   *
   * <p>The same predicate serves the outgoing direction with {@code peerIp} set to the recipient's
   * address, which stops us from being an amplifier for entries we do accept locally.
   *
   * @param advertisedIp the address contained in the peer-list entry
   * @param advertisedPort the port contained in the peer-list entry
   * @param peerIp the address of the peer we received the entry from / are sending it to
   */
  public static boolean isPlausibleAdvertisedAddress(
      String advertisedIp, int advertisedPort, String peerIp) {
    if (advertisedIp == null || advertisedIp.isBlank()) {
      return false;
    }
    if (advertisedPort < 1 || advertisedPort > 65535) {
      // Port 0 is what a Peer carries while we only know the remote end of an inbound connection
      // and not its listening port. Such an entry can never be dialled by anyone, so passing it
      // around is pure noise — it made up the bulk of the observed peer-list growth.
      return false;
    }
    String normalized = normalizeHost(advertisedIp);
    if (normalized.equals("0.0.0.0") || normalized.equals("::")) {
      return false; // unspecified address, never a destination
    }
    if (!isLocalAddress(advertisedIp)) {
      return true;
    }
    // Local-only address: believable exactly from a peer that is itself local to us. An unknown
    // peer address (connection details already cleared) counts as not local.
    return peerIp != null && !peerIp.isBlank() && isLocalAddress(peerIp);
  }

  /**
   * Whether the given host is one of this machine's own addresses. Used together with our listening
   * port to drop peer-list entries that point back at us; the identity-based check upstream only
   * catches entries that carry our node id.
   */
  public static boolean isOwnHostAddress(String ip) {
    String host = normalizeHost(ip);
    if (host.isEmpty()) {
      return false;
    }
    Set<String> addresses = ownHostAddresses;
    if (addresses == null) {
      addresses = collectOwnHostAddresses();
      ownHostAddresses = addresses;
    }
    return addresses.contains(host);
  }

  private static volatile Set<String> ownHostAddresses;

  private static Set<String> collectOwnHostAddresses() {
    Set<String> addresses = new HashSet<>(List.of("localhost", "127.0.0.1", "::1"));
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces != null && interfaces.hasMoreElements()) {
        Enumeration<InetAddress> inetAddresses = interfaces.nextElement().getInetAddresses();
        while (inetAddresses.hasMoreElements()) {
          addresses.add(normalizeHost(inetAddresses.nextElement().getHostAddress()));
        }
      }
    } catch (SocketException e) {
      // no interface list available, the loopback defaults above still apply
    }
    return addresses;
  }

  /** Lower-cases the host and strips IPv6 brackets and a zone index, so hosts compare equal. */
  private static String normalizeHost(String host) {
    if (host == null) {
      return "";
    }
    String normalized = host.trim().toLowerCase(Locale.ROOT);
    if (normalized.startsWith("[") && normalized.endsWith("]") && normalized.length() > 2) {
      normalized = normalized.substring(1, normalized.length() - 1);
    }
    int zone = normalized.indexOf('%');
    if (zone >= 0) {
      normalized = normalized.substring(0, zone);
    }
    return normalized;
  }

  /** Parses a dotted-quad IPv4 literal, or null if the string is not one. */
  private static int[] parseIpv4(String host) {
    String[] parts = host.split("\\.", -1);
    if (parts.length != 4) {
      return null;
    }
    int[] octets = new int[4];
    for (int i = 0; i < 4; i++) {
      if (parts[i].isEmpty() || parts[i].length() > 3) {
        return null;
      }
      for (int c = 0; c < parts[i].length(); c++) {
        if (parts[i].charAt(c) < '0' || parts[i].charAt(c) > '9') {
          return null;
        }
      }
      octets[i] = Integer.parseInt(parts[i]);
      if (octets[i] > 255) {
        return null;
      }
    }
    return octets;
  }
}
