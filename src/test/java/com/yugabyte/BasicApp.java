package com.yugabyte;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;

public class BasicApp {
    private static int numConnections = 10;

    public static void main(String[] args) {

        ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?loadBalanceHosts=true");
//        ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?");

        long startTimefirstConn = System.nanoTime();

        Mono<Connection> firstConnection = Mono.from(connectionFactory.create());
        Connection firstConn = firstConnection.block();

        long startTime = System.nanoTime();
        ArrayList<Connection> connections = new ArrayList<>();

        for(int i = 0; i < numConnections - 1; i++){
            Mono<Connection> connectionMono = Mono.from(connectionFactory.create());

            Connection connection = connectionMono.block();

            connections.add(connection);
        }


//        Mono<Connection> secondConnection = Mono.from(connectionFactory.create());
//        Connection secondConn = secondConnection.block();

        long endTime = System.nanoTime();

        System.out.println("First Connection Created in " + ((startTime - startTimefirstConn)/1000000.0));
//        System.out.println("Second Connection Created in " + (endTime - startTime)/1000000.0);


        System.out.println("Connections Created in " + (endTime - startTime)/1000000.0);

        for (Connection connection: connections) {
            connection.close();
        }
    }
}