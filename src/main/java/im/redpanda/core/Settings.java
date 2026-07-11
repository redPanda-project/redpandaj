package im.redpanda.core;

import java.io.File;
import java.util.Arrays;

public class Settings {

  public static final int DEFAULT_PORT = 59558;
  public static boolean DEBUG = true;
  public static boolean NAT_OPEN = false;
  public static int STD_PORT = 59558;
  public static int MIN_CONNECTIONS = 20;
  public static int MAX_CONNECTIONS = 50;
  public static long pingTimeout = 65L * 1000L; // time in ms
  public static int pingDelay = 1000; // time in ms
  public static int peerListRequestDelay = 60 * 60; // time in sec
  public static boolean seedNode;
  public static boolean loadUpdates;

  public static final String SAVE_DIR = "data";

  public static final int k = 20; // k value from kademlia (nodes in one bucket)

  public static boolean IPV6_ONLY = false;
  public static boolean IPV4_ONLY = false;

  public static void init(ServerContext serverContext) {
    File file = new File("redpanda.jar");
    if (!file.exists()) {
      System.out.println("No jar to update found, disable auto update");
      loadUpdates = false;

      file = new File("target/redpanda.jar");
      if (file.exists() && serverContext.getPort() == DEFAULT_PORT) {
        System.out.println("found compiled jar, this is a seed node");
        seedNode = true;
      }
    } else {
      loadUpdates = true;
    }
  }

  private static final String[] DEFAULT_KNOWN_NODES = {
    "195.201.25.223:59558", "redpanda.im:59559", "127.0.0.1:59558"
  };

  /**
   * Bootstrap peers as {@code host:port}. Overridable without rebuilding via the system property
   * {@code redpanda.knownNodes} (same key the E2E launcher uses) or the environment variable {@code
   * REDPANDA_KNOWN_NODES}, both as a comma-separated list; the property wins over the environment,
   * blank values fall back to the defaults.
   */
  public static String[] knownNodes =
      parseKnownNodes(
          System.getProperty("redpanda.knownNodes", System.getenv("REDPANDA_KNOWN_NODES")));

  static String[] parseKnownNodes(String configured) {
    if (configured == null) {
      return DEFAULT_KNOWN_NODES.clone();
    }
    String[] entries =
        Arrays.stream(configured.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .filter(Settings::isValidKnownNode)
            .toArray(String[]::new);
    return entries.length == 0 ? DEFAULT_KNOWN_NODES.clone() : entries;
  }

  /** Accepts {@code host:port} or a bracketed IPv6 literal, matching what reseeding can parse. */
  private static boolean isValidKnownNode(String entry) {
    if (entry.startsWith("[")) {
      if (entry.endsWith("]") && entry.length() > 2) {
        return true;
      }
      return warnInvalidKnownNode(entry);
    }
    String[] split = entry.split(":");
    if (split.length != 2 || split[0].isEmpty()) {
      return warnInvalidKnownNode(entry);
    }
    try {
      int port = Integer.parseInt(split[1]);
      if (port < 1 || port > 65535) {
        return warnInvalidKnownNode(entry);
      }
    } catch (NumberFormatException e) {
      return warnInvalidKnownNode(entry);
    }
    return true;
  }

  private static boolean warnInvalidKnownNode(String entry) {
    System.err.println("ignoring invalid known-node entry: '" + entry + "' (expected host:port)");
    return false;
  }

  public static String[] blacklistIps = {};

  public static int getStartPort() {
    return STD_PORT;
  }

  public static boolean isSeedNode() {
    return seedNode;
  }

  public static boolean isLoadUpdates() {
    return loadUpdates;
  }
}
