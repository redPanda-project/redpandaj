package im.redpanda.updater;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.core.LocalSettings;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The plain-HTTP side-load endpoint (T121: TD128, TD129).
 *
 * <p>The apk path is redirected into a {@link TempDir} via {@code redpanda.update.apk.path}, which
 * is also the point of half these tests: before T121 the handler read a hard-coded {@code
 * Path.of("android.apk")} and ignored the override entirely.
 */
class HTTPServerTest {

  private static final int TEST_PORT = 49793;
  private static final String APK_PATH_PROPERTY = "redpanda.update.apk.path";

  @TempDir File tempDir;

  private File apkFile;
  private ServerContext ctx;
  private HTTPServer httpServer;

  @BeforeEach
  void setup() {
    apkFile = new File(tempDir, "android.apk");
    System.setProperty(APK_PATH_PROPERTY, apkFile.getAbsolutePath());
    ctx = ServerContext.buildDefaultServerContext();
    ctx.setPort(TEST_PORT);
  }

  @AfterEach
  void cleanup() {
    if (httpServer != null) {
      httpServer.stopServer();
      httpServer = null;
    }
    System.clearProperty(APK_PATH_PROPERTY);
    LocalSettings.settingsFile(TEST_PORT).delete();
  }

  /** Starts the endpoint on an ephemeral port and returns its base URL. */
  private String startOnEphemeralPort() {
    httpServer = new HTTPServer(ctx, 0);
    httpServer.run(); // run(), not start(): binding synchronously makes boundPort() usable at once
    int port = httpServer.boundPort();
    assertNotEquals(-1, port, "the endpoint must be listening");
    return "http://127.0.0.1:" + port + "/android.apk.signed";
  }

  private static HttpResponse<byte[]> get(String url) throws Exception {
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    return client.send(
        HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
        HttpResponse.BodyHandlers.ofByteArray());
  }

  /** A syntactically valid (fixed 64-byte Ed25519) but cryptographically fake signature. */
  private static byte[] fakeSignature() {
    byte[] sig = new byte[NodeId.SIGNATURE_LEN];
    for (int i = 0; i < sig.length; i++) sig[i] = (byte) (i + 1);
    return sig;
  }

  /**
   * TD128. The handler must read the apk through {@link UpdateTransfer#updateApkPath()}, so the
   * path overrides reach it — before T121 it read a hard-coded {@code android.apk} from the working
   * directory and this test would have served whatever happened to be there (or 404'd). The frame
   * is command 16 without the command byte, so an app verifies exactly what a peer would.
   */
  @Test
  void servesTheSignedApkFromTheConfiguredPath() throws Exception {
    byte[] apk = "not-really-an-apk".getBytes();
    Files.write(apkFile.toPath(), apk);
    byte[] signature = fakeSignature();
    long timestamp = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    ctx.getLocalSettings().setUpdateAndroidSignature(signature);
    ctx.getLocalSettings().setUpdateAndroidTimestamp(timestamp);

    HttpResponse<byte[]> response = get(startOnEphemeralPort());

    assertEquals(200, response.statusCode());
    assertEquals(
        "application/octet-stream", response.headers().firstValue("Content-Type").orElse(null));
    ByteBuffer body = ByteBuffer.wrap(response.body());
    assertEquals(timestamp, body.getLong(), "timestamp");
    assertEquals(apk.length, body.getInt(), "apk length");
    byte[] servedSignature = new byte[NodeId.SIGNATURE_LEN];
    body.get(servedSignature);
    assertArrayEquals(signature, servedSignature, "signature");
    byte[] servedApk = new byte[apk.length];
    body.get(servedApk);
    assertArrayEquals(apk, servedApk, "apk bytes");
    assertEquals(0, body.remaining(), "nothing beyond the frame");
  }

  /**
   * TD128. Every node that has not received a signed apk yet has a null signature. That used to NPE
   * inside {@code handle()}, which the JDK server answers by dropping the connection — the client
   * sees a broken pipe, the node log a stack trace on stderr.
   */
  @Test
  void answers404WhenThereIsNoSignature() throws Exception {
    Files.write(apkFile.toPath(), "apk-without-a-signature".getBytes());
    assertEquals(null, ctx.getLocalSettings().getUpdateAndroidSignature(), "fresh settings");

    HttpResponse<byte[]> response = get(startOnEphemeralPort());

    assertEquals(404, response.statusCode());
    assertEquals(0, response.body().length);
  }

  /** TD128. Same for a signature without the file it belongs to. */
  @Test
  void answers404WhenTheApkFileIsMissing() throws Exception {
    ctx.getLocalSettings().setUpdateAndroidSignature(fakeSignature());
    ctx.getLocalSettings().setUpdateAndroidTimestamp(Updater.MIN_UPDATE_TIMESTAMP_MS + 1);
    assertTrue(!apkFile.exists(), "no apk on disk");

    HttpResponse<byte[]> response = get(startOnEphemeralPort());

    assertEquals(404, response.statusCode());
  }

  /**
   * TD129. {@code run()} caught the bind {@link IOException} with an empty body (the logging was
   * commented out), so a port-8081 conflict — a second node on the host, a leftover JVM — left the
   * endpoint dead with nothing in the log to say so.
   */
  @Test
  void logsTheBindFailure() throws Exception {
    List<LogEvent> events = new CopyOnWriteArrayList<>();
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    AbstractAppender appender =
        new AbstractAppender("HTTPServerTestAppender", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    org.apache.logging.log4j.core.config.LoggerConfig loggerConfig =
        new org.apache.logging.log4j.core.config.LoggerConfig(
            HTTPServer.class.getName(), Level.ALL, true);
    loggerConfig.addAppender(appender, Level.ALL, null);
    logContext.getConfiguration().addLogger(HTTPServer.class.getName(), loggerConfig);
    logContext.updateLoggers();

    try (ServerSocket occupied = new ServerSocket(0)) {
      httpServer = new HTTPServer(ctx, occupied.getLocalPort());
      httpServer.run();

      assertEquals(-1, httpServer.boundPort(), "the endpoint must not claim to be listening");
      assertTrue(
          events.stream()
              .anyMatch(
                  e ->
                      e.getLevel() == Level.ERROR
                          && e.getMessage().getFormattedMessage().contains("could not be bound")
                          && e.getThrown() instanceof IOException),
          "the bind failure must be logged with its exception, events: " + events);
    } finally {
      logContext.getConfiguration().removeLogger(HTTPServer.class.getName());
      logContext.updateLoggers();
      appender.stop();
    }
  }
}
