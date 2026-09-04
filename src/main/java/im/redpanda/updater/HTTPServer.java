package im.redpanda.updater;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import im.redpanda.core.ServerContext;
import im.redpanda.ops.Log;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Serves the locally stored, signed {@code android.apk} over plain HTTP so the app can be
 * side-loaded without speaking the peer wire protocol. Same frame as command 16 ({@code
 * ANDROID_UPDATE_ANSWER_CONTENT}) minus the command byte: {@code [8 timestamp][4 length][64
 * signature][apk]}, so the client verifies exactly what a peer would.
 */
public class HTTPServer extends Thread {

  private static final Logger logger = LogManager.getLogger();

  private final int PORT;
  private final ServerContext serverContext;

  /** The running server, or {@code null} before {@link #run()} and after a failed bind. */
  private volatile HttpServer server;

  public HTTPServer(ServerContext serverContext) {
    this.PORT = 8081;
    this.serverContext = serverContext;
  }

  public HTTPServer(ServerContext serverContext, int PORT) {
    this.PORT = PORT;
    this.serverContext = serverContext;
  }

  @Override
  public void run() {
    try {
      System.out.println("starting HTTP server...");
      HttpServer created = HttpServer.create(new InetSocketAddress(PORT), 10);
      created.createContext("/android.apk.signed", new HHandler());
      created.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
      created.start();
      server = created;
    } catch (IOException e) {
      // TD129: this was an empty catch with the logging commented out, so a port conflict (a
      // second node on the host, a leftover JVM) silently left the apk endpoint dead — nothing
      // in the node log distinguished that from a node that never had an apk to serve. The node
      // itself keeps running on purpose: the peer wire protocol still distributes the apk.
      logger.error("android.apk HTTP server not started: port {} could not be bound", PORT, e);
      Log.sentry(e);
    }
  }

  /** The port actually bound, or {@code -1} if the server is not running. Test/ops helper. */
  int boundPort() {
    HttpServer running = server;
    return running == null ? -1 : running.getAddress().getPort();
  }

  /** Stops the server if it is running. Test helper — production runs it for the JVM's life. */
  void stopServer() {
    HttpServer running = server;
    if (running != null) {
      running.stop(0);
      server = null;
    }
  }

  class HHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange t) throws IOException {
      // TD128: the path used to be a hard-coded Path.of("android.apk"), so the
      // redpanda.android.update.file / redpanda.update.apk.path overrides applied to the wire
      // path but not to this one — the HTTP endpoint could serve a different file than the one
      // the node distributes to peers, or none at all.
      Path path = UpdateTransfer.updateApkPath();
      byte[] signature = serverContext.getLocalSettings().getUpdateAndroidSignature();

      if (signature == null || !Files.isReadable(path)) {
        // TD128: a node with no signed apk (every node that has not received one yet) threw
        // NullPointerException / NoSuchFileException out of handle(), which the JDK server turns
        // into a dropped connection plus a stack trace on stderr. Say "we have nothing" instead.
        logger.info(
            "no signed android.apk to serve (signature present: {}, readable file at {}: {})",
            signature != null,
            path,
            Files.isReadable(path));
        t.sendResponseHeaders(404, -1);
        t.close();
        return;
      }

      byte[] data = Files.readAllBytes(path);
      long timestamp = serverContext.getLocalSettings().getUpdateAndroidTimestamp();

      ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + signature.length + data.length);
      buffer.putLong(timestamp);
      buffer.putInt(data.length);
      buffer.put(signature);
      buffer.put(data);
      byte[] body = buffer.array();

      Headers h = t.getResponseHeaders();
      h.add("Content-Type", "application/octet-stream");

      t.sendResponseHeaders(200, body.length);
      try (OutputStream os = t.getResponseBody()) {
        os.write(body);
      }
    }
  }
}
