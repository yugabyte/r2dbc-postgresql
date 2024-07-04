package com.yugabyte;


import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import java.util.HashMap;
import java.util.Map;

public class ConcurrentConnectionsTest extends UniformLoadbalancerTest{
    protected static int numConnectionsPerThread = 2;
    protected static int numThreads = 24;
    protected static final String path = System.getenv("YBDB_PATH");

    public static void main(String[] args) throws  InterruptedException {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalStateException("YBDB_PATH not defined.");
        }

        int total = numThreads * numConnectionsPerThread;
        Map<String, Integer> expected = new HashMap<>();
        expected.put("127.0.0.1", total/3);
        expected.put("127.0.0.2", total/3);
        expected.put("127.0.0.3", total/3);
        controlConnection = "127.0.0.3";
        testConcurrentConnectionCreations(expected, null);

    }

    protected static void testConcurrentConnectionCreations(Map<String, Integer> expected1, String tkValues) throws
            InterruptedException {
        System.out.println("Running testConcurrentConnectionCreations()");
        startYBDBCluster();
        Thread.sleep(5000);
        try {
            System.out.println("Cluster started!");
            Thread.sleep(5000);
            int total = numThreads * numConnectionsPerThread;
            Thread[] threads = new Thread[numThreads];

            connectionFactory = tkValues == null ? new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                    .addHost("127.0.0.1")
                    .username("yugabyte")
                    .password("yugabyte")
                    .database("yugabyte")
                    .loadBalanceHosts(true)
                    .ybServersRefreshInterval(10)
                    .build()) :
                    new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                            .addHost("127.0.0.1")
                            .username("yugabyte")
                            .password("yugabyte")
                            .database("yugabyte")
                            .loadBalanceHosts(true)
                            .ybServersRefreshInterval(10)
                            .topologyKeys(tkValues)
                            .build())
                    ;
            PostgresqlConnection[] connections = new PostgresqlConnection[total];

            for (int i = 0 ; i < numThreads ; i++) {
                final int j = i;
                threads[i] = new Thread(() -> {
                    try {
                        connections[j] = connectionFactory.create().block();
                        connections[j + numThreads] = connectionFactory.create().block();
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                });
            }

            for (int i = 0 ; i < numThreads; i++) {
                threads[i].start();
            }
            System.out.println("Launched " + numThreads + " threads to create " + numConnectionsPerThread + " connections each");

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            for (Map.Entry<String, Integer> e : expected1.entrySet()) {
                verifyOn(e.getKey(), e.getValue());
            }

            System.out.println("Closing connections ...");
            for (int i = 0 ; i < total; i++) {
                connections[i].close().block();
            }

        } finally {
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
        }
    }

    protected static void startYBDBCluster() {
        executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
        executeCmd(path + "/bin/yb-ctl create --rf 3 --placement_info \"aws.us-west.us-west-2a,aws.us-west.us-west-2a,aws.us-west.us-west-2b\"", "Start YugabyteDB rf=3 cluster", 15);
    }
}
