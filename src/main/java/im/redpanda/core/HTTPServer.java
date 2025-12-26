package im.redpanda.core;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class HTTPServer extends Thread {

  private final int PORT;
  private final ServerContext serverContext;

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
      HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 10);
      server.createContext("/android.apk.signed", new HHandler());
      server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
      server.start();
    } catch (IOException e) {
      // Log.sentry(e);
      // e.printStackTrace();
    }
  }

  class HHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange t) throws IOException {
      Headers h = t.getResponseHeaders();

      Path path = Path.of("android.apk");
      byte[] data = Files.readAllBytes(path);

      long timestamp = serverContext.getLocalSettings().getUpdateAndroidTimestamp();
      byte[] signature = serverContext.getLocalSettings().getUpdateAndroidSignature();

      ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + signature.length + data.length);
      buffer.putLong(timestamp);
      buffer.putInt(data.length);
      buffer.put(signature);
      buffer.put(data);

      data = buffer.array();

      // h.add("Content-Type", "application/json");
      h.add("Content-Type", "application/octet-stream");

      t.sendResponseHeaders(200, data.length);
      OutputStream os = t.getResponseBody();
      os.write(data);
      os.close();
    }
  }
}
