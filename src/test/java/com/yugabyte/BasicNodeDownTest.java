package com.yugabyte;

import io.r2dbc.postgresql.api.PostgresqlConnection;

import java.util.ArrayList;
import java.util.Arrays;

public class BasicNodeDownTest extends UniformLoadbalancerTest{

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

        executeCmd(path + "/bin/yb-ctl stop_node 1", "Stop Node 1 of the cluster", 10);
        Thread.sleep(10);

        for (int i = 0; i < numConnections; i++) {
            PostgresqlConnection connection = connectionFactory.create().block();
            connections.add(connection);
        }

        verifyConns(Arrays.asList(-1, 6, 6));

        for (PostgresqlConnection connection: connections) {
            connection.close().block();
        }
    }
}
