package im.redpanda.e2e;

import im.redpanda.testutil.TestNodeProcess;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
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

            Thread.sleep(2000L);

            nodeA.stop(Duration.ofSeconds(10));
            nodeB.stop(Duration.ofSeconds(10));

            assertEquals("Node A exit code", 0, nodeA.exitCode());
            assertEquals("Node B exit code", 0, nodeB.exitCode());

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
