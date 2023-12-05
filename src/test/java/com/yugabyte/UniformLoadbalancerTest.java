package com.yugabyte;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.UniformLoadBalancerConnectionStrategy;
import io.r2dbc.postgresql.api.PostgresqlConnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class UniformLoadbalancerTest {

    private static int numConnections = 12;
    private static final String path = System.getenv("YBDB_PATH");

    private static PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .addHost("127.0.0.3")
            .username("yugabyte")
            .password("yugabyte")
            .database("yugabyte")
            .loadBalanceHosts(true)
            .ybserversrefreshinterval(10)
            .build());

    public static void main (String args[]) throws InterruptedException {
        checkBasicBehaviour();
        checkNodeDownBehaviour();
        checkAddNodeBehaviour();
    }

    private static void checkBasicBehaviour() throws InterruptedException {
        startYBDBCluster();

        Thread.sleep(5000);
        ArrayList<PostgresqlConnection> connections = new ArrayList<>();

        for (int i = 0; i < numConnections; i++) {
            PostgresqlConnection connection = connectionFactory.create().block();
            connections.add(connection);
        }

        verifyConns(Arrays.asList(4, 4, 4));

        for (PostgresqlConnection connection: connections) {
            connection.close().block();
        }
    }

    private static void checkNodeDownBehaviour() throws InterruptedException {
        startYBDBCluster();

        Thread.sleep(5000);
        ArrayList<PostgresqlConnection> connections = new ArrayList<>();

        for (int i = 0; i < numConnections; i++) {
            PostgresqlConnection connection = connectionFactory.create().block();
            connections.add(connection);
        }

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

    private static void checkAddNodeBehaviour() throws InterruptedException {

        startYBDBCluster();

        Thread.sleep(5000);
        ArrayList<PostgresqlConnection> connections = new ArrayList<>();

        for (int i = 0; i < numConnections; i++) {
            PostgresqlConnection connection = connectionFactory.create().block();
            connections.add(connection);
        }

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

        verifyConns(Arrays.asList(3, 3, 3, 3));

        for (PostgresqlConnection connection: connections) {
            connection.close().block();
        }

    }

    protected static void startYBDBCluster() {
        executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
        executeCmd(path + "/bin/yb-ctl create --rf 3", "Start YugabyteDB rf=3 cluster", 15);
    }

    protected static void executeCmd(String cmd, String msg, int timeout) {
        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("sh", "-c", cmd);
            Process process = builder.start();
            process.waitFor(timeout, TimeUnit.SECONDS);
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                throw new RuntimeException(msg + ": FAILED");
            }
            System.out.println(msg + ": SUCCEEDED!");
        } catch (Exception e) {
            System.out.println("Exception " + e);
        }
    }

    protected static void verifyConns(List<Integer> expectedCounts){
        int j = 1;
        System.out.println("Client backend processes on ");
        for (int expectedCount : expectedCounts){
            if(expectedCount != -1){
                verifyOn("127.0.0."+j, expectedCount);
            }
            j++;
        }
    }

    protected static void verifyOn(String server, int expectedCount) {
        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("sh", "-c", "curl http://" + server + ":13000/rpcz");
            Process process = builder.start();
            String result = new BufferedReader(new InputStreamReader(process.getInputStream()))
                    .lines().collect(Collectors.joining("\n"));
            process.waitFor(10, TimeUnit.SECONDS);
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                throw new RuntimeException("Could not access /rpcz on " + server);
            }
            String[] count = result.split("client backend");
            System.out.println(server + " = " + (count.length - 1));
            // Server side validation
            if (expectedCount != (count.length - 1)) {
                throw new RuntimeException("Client backend processes did not match on host:"+ server+". (expected, actual): "
                        + expectedCount + ", " + (count.length - 1));
            }
        } catch (InterruptedException | IOException e) {
            System.out.println(e);
        }
    }

}
