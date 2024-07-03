package com.yugabyte;

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;

public class URLTest extends UniformLoadbalancerTest{

    private static final String path = System.getenv("YBDB_PATH");
    private static int numConnections = 12;

    public static void main(String args[]) throws InterruptedException {

        startYBDBCluster();

        controlConnection = "127.0.0.3";

        Thread.sleep(5000);

        ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:postgresql://yugabyte:yugabyte@127.0.0.3:5433/yugabyte?loadBalanceHosts=true");

        ArrayList<Connection> connections = new ArrayList<>();

        for(int i = 0; i < numConnections; i++){
            Mono<Connection> connectionMono = Mono.from(connectionFactory.create());

            Connection connection = connectionMono.block();

            connections.add(connection);
        }

        verifyConns(Arrays.asList(4, 4, 4));

        for (Connection connection: connections) {
            connection.close();
        }
    }
}
