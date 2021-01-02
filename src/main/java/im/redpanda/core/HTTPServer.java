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
import java.nio.file.Paths;

public class HTTPServer extends Thread {

    private final int PORT;

    public HTTPServer() {
        this.PORT = 8081;
    }

    public HTTPServer(int PORT) {
        this.PORT = PORT;
    }

    @Override
    public void run() {
        try {
            System.out.println("starting HTTP server...");
            HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 10);
            server.createContext("/android.apk.signed", new HHandler());
            //server.setExecutor(null); // creates a default executor
            server.start();
        } catch (IOException e) {
//            Log.sentry(e);
//            e.printStackTrace();
        }
    }

    static class HHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            Headers h = t.getResponseHeaders();


            Path path = Paths.get("android.apk");
            byte[] data = Files.readAllBytes(path);


            long timestamp = Server.localSettings.getUpdateAndroidTimestamp();
            byte[] signature = Server.localSettings.getUpdateAndroidSignature();

            ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + signature.length + data.length);
            buffer.putLong(timestamp);
            buffer.putInt(data.length);
            buffer.put(signature);
            buffer.put(data);

            data = buffer.array();


//            h.add("Content-Type", "application/json");
            h.add("Content-Type", "application/octet-stream");

            t.sendResponseHeaders(200, data.length);
            OutputStream os = t.getResponseBody();
            os.write(data);
            os.close();
        }
    }
}

