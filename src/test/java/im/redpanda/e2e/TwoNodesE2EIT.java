package im.redpanda.e2e;

import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TwoNodesE2EIT {

    @Test
    public void startTwoNodes_logToFiles_andKillAfter10s() throws Exception {
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String mainClass = "im.redpanda.App";

        Path runDir = Paths.get("target", "e2e", "run-" + System.currentTimeMillis());
        Path node1Dir = runDir.resolve("node1");
        Path node2Dir = runDir.resolve("node2");
        Files.createDirectories(node1Dir);
        Files.createDirectories(node2Dir);
        File log1 = node1Dir.resolve("node1.log").toFile();
        File log2 = node2Dir.resolve("node2.log").toFile();

        List<String> cmd = Arrays.asList(javaBin, "-cp", classpath, mainClass);

        ProcessBuilder pb1 = new ProcessBuilder(cmd);
        // Run node1 on the standard port so seeds target it
        pb1.environment().put("PORT", String.valueOf(59558));
        pb1.redirectOutput(ProcessBuilder.Redirect.appendTo(log1));
        pb1.redirectError(ProcessBuilder.Redirect.appendTo(log1));
        pb1.directory(node1Dir.toFile());

        ProcessBuilder pb2 = new ProcessBuilder(cmd);
        pb2.environment().put("PORT", "60001");
        pb2.redirectOutput(ProcessBuilder.Redirect.appendTo(log2));
        pb2.redirectError(ProcessBuilder.Redirect.appendTo(log2));
        pb2.directory(node2Dir.toFile());

        Process p1 = null;
        Process p2 = null;
        try {
            p1 = pb1.start();
            p2 = pb2.start();

            // Wait until at least one node reports a successful connection, up to 20s
            long start = System.currentTimeMillis();
            boolean connected = false;
            while (System.currentTimeMillis() - start < 20_000L) {
                if (log1.exists() && log1.length() > 0 && Files.readString(log1.toPath(), StandardCharsets.UTF_8).contains("Connected successfully to ")) {
                    connected = true;
                    break;
                }
                if (log2.exists() && log2.length() > 0 && Files.readString(log2.toPath(), StandardCharsets.UTF_8).contains("Connected successfully to ")) {
                    connected = true;
                    break;
                }
                Thread.sleep(500L);
            }
            assertTrue("Expected 'Connected successfully' to appear in at least one node's log within timeout", connected);
        } finally {
            if (p1 != null) {
                p1.destroy(); // try graceful
                if (!p1.waitFor(8, TimeUnit.SECONDS)) {
                    p1.destroyForcibly();
                    p1.waitFor(5, TimeUnit.SECONDS);
                }
            }
            if (p2 != null) {
                p2.destroy();
                if (!p2.waitFor(8, TimeUnit.SECONDS)) {
                    p2.destroyForcibly();
                    p2.waitFor(5, TimeUnit.SECONDS);
                }
            }
        }

        // Basic assertions: logs exist and are non-empty
        assertTrue("node1.log should exist", log1.exists());
        assertTrue("node2.log should exist", log2.exists());
        assertTrue("node1.log should be non-empty", log1.length() > 0);
        assertTrue("node2.log should be non-empty", log2.length() > 0);

        // Scan logs for errors/exceptions, allow expected first-run LocalSettings FileNotFound only
        String log1Content = Files.readString(log1.toPath(), StandardCharsets.UTF_8);
        String log2Content = Files.readString(log2.toPath(), StandardCharsets.UTF_8);

        List<String> offending = new ArrayList<>();
        for (String line : log1Content.split("\n")) {
            if (isOffending(line)) offending.add("node1: " + line);
        }
        for (String line : log2Content.split("\n")) {
            if (isOffending(line)) offending.add("node2: " + line);
        }

        assertTrue("Unexpected errors/warnings in logs:\n" + String.join("\n", offending), offending.isEmpty());

        // Connectivity assertion: the explicit success line should be present in at least one log
        boolean node1Connected = log1Content.contains("Connected successfully to ");
        boolean node2Connected = log2Content.contains("Connected successfully to ");
        assertTrue("Expected explicit 'Connected successfully' in at least one node log",
                node1Connected || node2Connected);
    }

    private static boolean isOffending(String line) {
        String l = line.trim();
        // Quickly ignore empty lines
        if (l.isEmpty()) return false;

        // Allow benign first-run missing LocalSettings file
        if ((l.contains("java.io.FileNotFoundException") && l.contains("data/localSettings"))
                || l.contains("LocalSettings -- error loading local settings")) {
            return false;
        }

        // Allow NPE from ListenConsole in headless tests (no stdin)
        if (l.contains("NullPointerException") && l.contains("ListenConsole")) {
            return false;
        }

        // Disallow MapDB header warnings from unclean shutdown
        if (l.contains("Header checksum broken")) {
            return true;
        }

        // Flag typical error/warn indicators
        String u = l.toUpperCase();
        if (u.contains(" EXCEPTION") || u.contains("ERROR") || u.contains("FATAL") || u.contains("SEVERE") || u.contains("WARNING") || u.contains("WARN ")) {
            return true;
        }
        return false;
    }
}
