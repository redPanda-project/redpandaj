package im.redpanda.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.testutil.TestNodeProcess;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TwoNodesE2EIT {

  @TempDir public File temporaryFolder;

  @Test
  void nodesStartLogCleanlyAndShutDown() throws Exception {
    Path nodeADir = newFolder(temporaryFolder, "nodeA").toPath();
    Path nodeBDir = newFolder(temporaryFolder, "nodeB").toPath();

    int portA = nextFreePort();
    int portB = nextFreePort();

    try (TestNodeProcess nodeA = TestNodeProcess.start(nodeADir, portA, "", 0);
        TestNodeProcess nodeB = TestNodeProcess.start(nodeBDir, portB, "", 0)) {

      assertTrue(nodeA.awaitReady(Duration.ofSeconds(30)), "Node A failed to announce readiness");
      assertTrue(nodeB.awaitReady(Duration.ofSeconds(30)), "Node B failed to announce readiness");

      CountDownLatch pause = new CountDownLatch(1);
      pause.await(2, TimeUnit.SECONDS);

      nodeA.stop(Duration.ofSeconds(10));
      nodeB.stop(Duration.ofSeconds(10));

      String nodeAOutput = nodeA.getCombinedOutput();
      String nodeBOutput = nodeB.getCombinedOutput();

      if (nodeA.exitCode() != 0 || nodeB.exitCode() != 0) {
        System.out.println("Node A output:\n" + nodeAOutput);
        System.out.println("Node B output:\n" + nodeBOutput);
        Path logDir = Path.of("target", "e2e-logs");
        Files.createDirectories(logDir);
        Files.writeString(logDir.resolve("nodeA.log"), nodeAOutput);
        Files.writeString(logDir.resolve("nodeB.log"), nodeBOutput);
      }

      assertEquals(0, nodeA.exitCode(), "Node A exit code\n" + nodeAOutput);
      assertEquals(0, nodeB.exitCode(), "Node B exit code\n" + nodeBOutput);

      assertNoUnexpectedLogIssues("nodeA", nodeA.getCombinedOutput());
      assertNoUnexpectedLogIssues("nodeB", nodeB.getCombinedOutput());
    }
  }

  private void assertNoUnexpectedLogIssues(String nodeLabel, String combinedOutput) {
    String[] lines = combinedOutput.split(System.lineSeparator());
    List<String> offenders = new ArrayList<>();
    for (String line : lines) {
      if (isAllowedNoise(line)) {
        continue;
      }
      if (line.contains("ERROR") || line.contains("WARN") || line.contains("Exception")) {
        offenders.add(line);
      }
    }
    assertThat(offenders)
        .as(nodeLabel + " had unexpected WARN/ERROR lines: " + offenders)
        .isEmpty();
  }

  private boolean isAllowedNoise(String line) {
    String lower = line.toLowerCase();
    if (lower.contains("error loading local settings")) {
      return true;
    }
    if (lower.contains("filenotfoundexception") && lower.contains("localsettings")) {
      return true;
    }
    if (line.contains("ListenConsole") && line.contains("NullPointerException")) {
      return true;
    }
    return false;
  }

  private int nextFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
