package im.redpanda.e2e;

import im.redpanda.testutil.TestNodeProcess;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TwoNodesE2EIT {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void nodesStartLogCleanlyAndShutDown() throws Exception {
        Path nodeADir = temporaryFolder.newFolder("nodeA").toPath();
        Path nodeBDir = temporaryFolder.newFolder("nodeB").toPath();

        int portA = nextFreePort();
        int portB = nextFreePort();

        try (TestNodeProcess nodeA = TestNodeProcess.start(nodeADir, portA, "", 0);
             TestNodeProcess nodeB = TestNodeProcess.start(nodeBDir, portB, "", 0)) {

            assertTrue("Node A failed to announce readiness", nodeA.awaitReady(Duration.ofSeconds(30)));
            assertTrue("Node B failed to announce readiness", nodeB.awaitReady(Duration.ofSeconds(30)));

            CountDownLatch pause = new CountDownLatch(1);
            pause.await(2, TimeUnit.SECONDS);

            nodeA.stop(Duration.ofSeconds(10));
            nodeB.stop(Duration.ofSeconds(10));

            String nodeAOutput = nodeA.getCombinedOutput();
            String nodeBOutput = nodeB.getCombinedOutput();

            if (nodeA.exitCode() != 0 || nodeB.exitCode() != 0) {
                System.out.println("Node A output:\n" + nodeAOutput);
                System.out.println("Node B output:\n" + nodeBOutput);
                Path logDir = Paths.get("target", "e2e-logs");
                Files.createDirectories(logDir);
                Files.writeString(logDir.resolve("nodeA.log"), nodeAOutput);
                Files.writeString(logDir.resolve("nodeB.log"), nodeBOutput);
            }

            assertEquals("Node A exit code\n" + nodeAOutput, 0, nodeA.exitCode());
            assertEquals("Node B exit code\n" + nodeBOutput, 0, nodeB.exitCode());

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
        assertThat(nodeLabel + " had unexpected WARN/ERROR lines: " + offenders, offenders, is(empty()));
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
}
