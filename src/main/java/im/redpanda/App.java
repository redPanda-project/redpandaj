package im.redpanda;

import im.redpanda.core.Server;
import io.sentry.Sentry;
import io.sentry.event.UserBuilder;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Starting redpanda " + new App().getClass().getPackage().getImplementationVersion());


        Sentry.init("https://eefa8afdcdb7418995f6306c136546c7@sentry.io/1400313");

        Sentry.getContext().addTag("release", new App().getClass().getPackage().getImplementationVersion());

        Server.start();

    }


    public String getHelloWorld() {
        return "Hello World!";
    }


}
