package im.redpanda;

import im.redpanda.core.Server;
import io.sentry.Sentry;
import io.sentry.event.UserBuilder;

import java.io.IOException;
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

        Enumeration<URL> resources = new App().getClass().getClassLoader()
                .getResources("META-INF/MANIFEST.MF");
        while (resources.hasMoreElements()) {
            try {
                Manifest manifest = new Manifest(resources.nextElement().openStream());
                // check that this is your manifest and do what you need or get the next one

//                System.out.println(" build number: " + manifest.getMainAttributes().getValue("Implementation-Build"));

            } catch (IOException E) {
                // handle
            }
        }


        Sentry.init("https://eefa8afdcdb7418995f6306c136546c7@sentry.io/1400313");

        Sentry.getContext().addTag("release", new App().getClass().getPackage().getImplementationVersion());

        Server.start();

    }


    public String getHelloWorld() {
        return "Hello World!";
    }


}
