package im.redpanda.testutil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Starts and manages a test node in a separate JVM so static singletons do not collide across
 * nodes.
 */
public final class TestNodeProcess implements AutoCloseable {

  private final Process process;
  private final StringBuilder stdout = new StringBuilder();
  private final StringBuilder stderr = new StringBuilder();
  private final CountDownLatch readyLatch = new CountDownLatch(1);
  private Thread stdoutReader;
  private Thread stderrReader;
  private final OutputStreamWriter stdinWriter;

  private TestNodeProcess(Process process, OutputStreamWriter stdinWriter) {
    this.process = process;
    this.stdinWriter = stdinWriter;
  }

  public static TestNodeProcess start(Path workDir, int port, String knownNodes, int minConnections)
      throws IOException {
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add(
        "-Dlog4j.configurationFile=" + Path.of("src/test/resources/log4j2.xml").toAbsolutePath());
    command.add("-Dredpanda.workdir=" + workDir.toAbsolutePath());
    command.add("-Dredpanda.knownNodes=" + knownNodes);
    command.add("-Dredpanda.minConnections=" + minConnections);
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add("im.redpanda.testutil.TestNodeLauncher");

    ProcessBuilder builder = new ProcessBuilder(command);
    builder.directory(workDir.toFile());
    builder.redirectErrorStream(false);
    builder.environment().put("PORT", Integer.toString(port));

    Process process = builder.start();
    TestNodeProcess handle =
        new TestNodeProcess(
            process, new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
    handle.startReaders();
    return handle;
  }

  private void startReaders() {
    stdoutReader = new Thread(() -> readLines(process.getInputStream(), stdout, true));
    stderrReader = new Thread(() -> readLines(process.getErrorStream(), stderr, false));
    stdoutReader.setDaemon(true);
    stderrReader.setDaemon(true);
    stdoutReader.start();
    stderrReader.start();
  }

  private void readLines(java.io.InputStream stream, StringBuilder target, boolean watchReady) {
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        appendLine(target, line);
        if (watchReady && line.contains("NODE_READY")) {
          readyLatch.countDown();
        }
      }
    } catch (IOException e) {
      if (watchReady) {
        readyLatch.countDown(); // stream closed before readiness marker
      }
    }
  }

  private static void appendLine(StringBuilder target, String line) {
    synchronized (target) {
      target.append(line).append(System.lineSeparator());
    }
  }

  public boolean awaitReady(Duration timeout) throws InterruptedException {
    return readyLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void stop(Duration timeout) throws IOException, InterruptedException {
    if (!process.isAlive()) {
      return;
    }
    try {
      stdinWriter.write("stop\n");
      stdinWriter.flush();
      stdinWriter
          .close(); // also signal EOF so the launcher unblocks even if the stop line is missed
    } catch (IOException ignored) {
      // process may already be tearing down
    }
    if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS) && process.isAlive()) {
      process.destroy();
      process.waitFor(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
    }
    if (process.isAlive()) {
      process.destroyForcibly();
      process.waitFor(5, TimeUnit.SECONDS);
    }
    if (stdoutReader != null) {
      stdoutReader.join(TimeUnit.SECONDS.toMillis(1));
    }
    if (stderrReader != null) {
      stderrReader.join(TimeUnit.SECONDS.toMillis(1));
    }
  }

  public String getStdout() {
    synchronized (stdout) {
      return stdout.toString();
    }
  }

  public String getStderr() {
    synchronized (stderr) {
      return stderr.toString();
    }
  }

  public String getCombinedOutput() {
    return getStdout() + getStderr();
  }

  public int exitCode() {
    if (process.isAlive()) {
      return -1;
    }
    return process.exitValue();
  }

  @Override
  public void close() throws IOException {
    try {
      stop(Duration.ofSeconds(5));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
