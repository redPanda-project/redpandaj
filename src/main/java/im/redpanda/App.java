package im.redpanda;

import im.redpanda.core.Server;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Starting redpanda " + new App().getClass().getPackage().getImplementationVersion());


        Server.start();

    }


    public String getHelloWorld() {
        return "Hello World!";
    }


}
