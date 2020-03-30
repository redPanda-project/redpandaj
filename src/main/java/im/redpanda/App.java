package im.redpanda;

import im.redpanda.core.ListenConsole;
import im.redpanda.core.Server;
import io.sentry.Sentry;
import io.sentry.event.UserBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        System.out.print("Starting redpanda " + new App().getClass().getPackage().getImplementationVersion());

//        Enumeration<URL> resources = new App().getClass().getClassLoader()
//                .getResources("META-INF/MANIFEST.MF");
//        while (resources.hasMoreElements()) {
//            try {
//                Manifest manifest = new Manifest(resources.nextElement().openStream());
//                // check that this is your manifest and do what you need or get the next one
//
////                System.out.println(" build number: " + manifest.getMainAttributes().getValue("Implementation-Build"));
//
//            } catch (IOException E) {
//                // handle
//            }
//        }


        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                final String orgName = Thread.currentThread().getName();
                Thread.currentThread().setName(orgName + " - shutdownhook");
                Server.nodeStore.saveToDisk();
                System.out.println("started shutdownhook...");
                Server.shutdown();
                System.out.println("shutdownhook done");
            }
        });


        Sentry.init("https://eefa8afdcdb7418995f6306c136546c7@sentry.io/1400313");

        String gitRev = readGitProperties();
        if (gitRev != null) {
            Sentry.getContext().addTag("gitRev", gitRev);
            System.out.println("found git revision: " + gitRev);
        }


        Server.start();

        new ListenConsole().start();

    }


    public String getHelloWorld() {
        return "Hello World!";
    }

    public static String readGitProperties() throws IOException {
        String gitRev = null;
        InputStream is = new App().getClass().getResourceAsStream("/git.properties");
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
