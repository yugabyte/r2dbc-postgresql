package com.yugabyte;

import io.r2dbc.postgresql.api.PostgresqlConnection;

import java.util.ArrayList;
import java.util.Arrays;

public class BasicAddNodeTest extends UniformLoadbalancerTest{

    private static final String path = System.getenv("YBDB_PATH");

    public static void main(String args[]) throws InterruptedException {

        startYBDBCluster();

        Thread.sleep(5000);
        ArrayList<PostgresqlConnection> connections = new ArrayList<>();

        for (int i = 0; i < numConnections; i++) {
            PostgresqlConnection connection = connectionFactory.create().block();
            connections.add(connection);
        }
        controlConnection = "127.0.0.3";
        verifyConns(Arrays.asList(4, 4, 4));

        for (PostgresqlConnection connection: connections) {
            connection.close().block();
        }

        connections.clear();

        executeCmd(path + "/bin/yb-ctl add_node", "Adding 1 node to the cluster", 10);
        Thread.sleep(10000);

        for (int i = 0; i < numConnections; i++) {
            PostgresqlConnection connection = connectionFactory.create().block();
            connections.add(connection);
        }

        controlConnection = "127.0.0.3";
        verifyConns(Arrays.asList(3, 3, 3, 3));

        for (PostgresqlConnection connection: connections) {
            connection.close().block();
        }

    }
}
