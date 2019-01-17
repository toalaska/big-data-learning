package net.toalaska;

import java.io.IOException;

import java.net.URISyntaxException;


public class App {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        WordCount wordCount =new WordCount();

        wordCount.setUp();

        wordCount.run();

    }


}
/*
 *
 * mvn clean package -DskipTests
 * */