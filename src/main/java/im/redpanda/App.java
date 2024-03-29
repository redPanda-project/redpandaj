package im.redpanda;

import im.redpanda.core.ByteBufferPool;
import im.redpanda.core.ConnectionHandler;
import im.redpanda.core.ListenConsole;
import im.redpanda.core.LocalSettings;
import im.redpanda.core.Log;
import im.redpanda.core.Node;
import im.redpanda.core.PeerJobs;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.GMManagerCleanJobs;
import im.redpanda.jobs.KadRefreshJob;
import im.redpanda.jobs.NodeConnectionPointsSeenJob;
import im.redpanda.jobs.NodeInfoSetRefreshJob;
import im.redpanda.jobs.SaveJobs;
import im.redpanda.jobs.ServerRestartJob;
import im.redpanda.jobs.UpTimeReporterJob;
import im.redpanda.store.NodeStore;
import io.sentry.Sentry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


/**
 * Hello world!
 */
public class App {


    private static final Logger logger = LogManager.getLogger();


    public static boolean sentryAllowed = false;

    public static void main(String[] args) throws IOException {

        System.out.println("               _ _____                _       \n" +
                "              | |  __ \\              | |      \n" +
                "  _ __ ___  __| | |__) |_ _ _ __   __| | __ _ \n" +
                " | '__/ _ \\/ _` |  ___/ _` | '_ \\ / _` |/ _` |\n" +
                " | | |  __/ (_| | |  | (_| | | | | (_| | (_| |\n" +
                " |_|  \\___|\\__,_|_|   \\__,_|_| |_|\\__,_|\\__,_|               - p2p chat, https://github.com/redPanda-project/redpandaj\n" +
                "                                              \n" +
                "                                              ");

        System.out.println("Starting redpanda " + App.class.getPackage().getImplementationVersion());


        initLogger();


        boolean activateSentry = false;
        if (args.length > 0) {
            String sentryExtras = args[0];
            String[] split = sentryExtras.split("=");
            if (split[1].equals("yes")) {
                activateSentry = true;
            }
        }


        logger.info("Activate Sentry: " + activateSentry);

        String gitRev = readGitProperties();

        if (activateSentry) {
            Sentry.init(options -> {
                options.setDsn("https://eefa8afdcdb7418995f6306c136546c7@sentry.io/1400313");
                options.setRelease(gitRev);
            });

            Sentry.configureScope(scope -> scope.setContexts("gitRev", gitRev));

            sentryAllowed = true;
        }

        if (gitRev != null) {
            logger.info("found git revision: " + gitRev + " Sentry: " + activateSentry);
        } else {
            logger.warn("Warning, no git revision found...");
        }


        ServerContext serverContext = new ServerContext();
        startServer(serverContext);

        new ListenConsole(serverContext).start();
    }

    private static void startServer(ServerContext serverContext) {
        ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, true);
        serverContext.setConnectionHandler(connectionHandler);
        int port = connectionHandler.bind();
        serverContext.setPort(port);
        serverContext.setLocalSettings(LocalSettings.load(serverContext.getPort()));
        serverContext.setNodeId(serverContext.getLocalSettings().getMyIdentity());
        serverContext.setNonce(serverContext.getLocalSettings().getMyIdentity().getKademliaId());
        serverContext.setNodeStore(NodeStore.buildWithDiskCache(serverContext));


        logger.info("started node with KademliaId: " + serverContext.getNonce().toString() + " port: " + serverContext.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                final String orgName = Thread.currentThread().getName();
                Thread.currentThread().setName(orgName + " - shutdownhook");
                logger.info("started shutdownhook...");
                Server.shutdown(serverContext);
                logger.info("shutdownhook done");
            }
        });

        //lets restart the server once in a while until we have stable releases...
        new ServerRestartJob(serverContext).start();

        ByteBufferPool.init();

        Server server = new Server(serverContext, connectionHandler);
        server.start();

        initServerNode(serverContext);

        Server.startUpRoutines(serverContext);

        Log.init(serverContext);

        startPermanentJobsWithServerContext(serverContext);
    }

    private static void initServerNode(ServerContext serverContext) {
        Node serverNode = serverContext.getNodeStore().get(serverContext.getNodeId().getKademliaId());
        if (serverNode == null) {
            serverNode = new Node(serverContext, serverContext.getNodeId());

        }
        serverContext.setNode(serverNode);
        serverContext.getNodeStore().getNodeGraph().addVertex(serverNode);
    }

    private static void startPermanentJobsWithServerContext(ServerContext serverContext) {
        new PeerJobs(serverContext).start();
        new SaveJobs(serverContext).start();
        new GMManagerCleanJobs(serverContext).start();
        new KadRefreshJob(serverContext).start();
        new NodeInfoSetRefreshJob(serverContext).start();
        new NodeConnectionPointsSeenJob(serverContext).start();
        new UpTimeReporterJob(serverContext).start();
    }

    private static void initLogger() {
        logger.info("redPanda started.");
    }


    public static String readGitProperties() throws IOException {
        String gitRev = null;
        InputStream is = App.class.getResourceAsStream("/git.properties");
        if (is == null) {
            return null;
        }
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line;
        while ((line = br.readLine()) != null) {

            if (line.startsWith("git.commit.id.abbrev=")) {
                gitRev = line.split("=")[1];
            }
        }
        br.close();
        isr.close();
        is.close();
        return gitRev;
    }


}
