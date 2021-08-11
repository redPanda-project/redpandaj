package im.redpanda;

import im.redpanda.core.*;
import im.redpanda.jobs.ServerRestartJob;
import io.sentry.Sentry;
import io.sentry.SentryClient;
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


    public static boolean SENTRY_ALLOWED = false;

    public static void main(String[] args) throws IOException {

        System.out.println("               _ _____                _       \n" +
                "              | |  __ \\              | |      \n" +
                "  _ __ ___  __| | |__) |_ _ _ __   __| | __ _ \n" +
                " | '__/ _ \\/ _` |  ___/ _` | '_ \\ / _` |/ _` |\n" +
                " | | |  __/ (_| | |  | (_| | | | | (_| | (_| |\n" +
                " |_|  \\___|\\__,_|_|   \\__,_|_| |_|\\__,_|\\__,_|               - p2p chat, https://github.com/redPanda-project/redpandaj\n" +
                "                                              \n" +
                "                                              ");

        System.out.println("Starting redpanda " + new App().getClass().getPackage().getImplementationVersion());


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

        SentryClient sentryClient;
        if (activateSentry) {
            sentryClient = Sentry.init("https://eefa8afdcdb7418995f6306c136546c7@sentry.io/1400313");
            SENTRY_ALLOWED = true;

            if (gitRev != null) {
                Sentry.getContext().addTag("gitRev", gitRev);
                sentryClient.setRelease(gitRev);
            }
        }

        if (gitRev != null) {
            logger.info("found git revision: " + gitRev + " Sentry: " + activateSentry);
        } else {
            logger.warn("Warning, no git revision found...");
        }


        ServerContext serverContext = new ServerContext();
        serverContext.setPeerList(new PeerList());

        ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, true);
        int port = connectionHandler.bind();
        serverContext.setPort(port);


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

        Server server = new Server(serverContext, connectionHandler);
        server.start();

        Log.init(serverContext);

        new PeerJobs(serverContext).start();

        new ListenConsole(serverContext).start();

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
