package com.yugabyte;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import reactor.core.publisher.Mono;

public class TopologyAwarePoolTest extends FallbackTopologyTest {

    private static final String path = System.getenv("YBDB_PATH");
    private static final int POOL_SIZE = 12;

    public static void main(String[] args) throws InterruptedException {
        startYBDBClusterWithSixNodes();

        try {
            controlConnection = "127.0.0.3";

            // Prefer zone 2a (nodes 1,2): all 12 pool connections land in zone 2a
            testPoolWithTopologyKeys(
                "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3",
                Arrays.asList(6, 6, 0, 0, 0, 0)
            );

            // Prefer zone 2b (nodes 3,4): all 12 pool connections land in zone 2b
            testPoolWithTopologyKeys(
                "aws.us-west.us-west-2b:1,aws.us-west.us-west-2a:2,aws.us-west.us-west-2c:3",
                Arrays.asList(0, 0, 6, 6, 0, 0)
            );

            // Prefer zone 2c (nodes 5,6): all 12 pool connections land in zone 2c
            testPoolWithTopologyKeys(
                "aws.us-west.us-west-2c:1,aws.us-west.us-west-2a:2,aws.us-west.us-west-2b:3",
                Arrays.asList(0, 0, 0, 0, 6, 6)
            );

            // Wildcard matches all zones equally: 2 connections per node
            testPoolWithTopologyKeys(
                "aws.us-west.*:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3",
                Arrays.asList(2, 2, 2, 2, 2, 2)
            );

        } finally {
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
            System.out.println("Done");
        }
    }

    private static void testPoolWithTopologyKeys(String topologyKeys, List<Integer> expected) throws InterruptedException {
        System.out.println("\n--- Testing pool with topology keys: " + topologyKeys + " ---");

        PostgresqlConnectionFactory driverFactory = new PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host("127.0.0.3")
                .port(5433)
                .database("yugabyte")
                .username("yugabyte")
                .password("yugabyte")
                .loadBalanceHosts(true)
                .ybServersRefreshInterval(10)
                .topologyKeys(topologyKeys)
                .build()
        );

        ConnectionPoolConfiguration config = ConnectionPoolConfiguration.builder(driverFactory)
            .initialSize(POOL_SIZE)
            .maxSize(POOL_SIZE)
            .maxIdleTime(Duration.ofMinutes(30))
            .build();

        ConnectionPool pool = new ConnectionPool(config);

        Thread.sleep(5000);

        Mono<String> op1 = runSlowQuery(pool, 1);
        Mono<String> op2 = runSlowQuery(pool, 2);
        Mono<String> op3 = runSlowQuery(pool, 3);

        System.out.println("Launching 3 concurrent operations on pool of size " + POOL_SIZE + "...");

        Mono.zip(op1, op2, op3)
            .doOnSuccess(tuple -> {
                System.out.println("All 3 operations completed:");
                System.out.println("  Op1: " + tuple.getT1());
                System.out.println("  Op2: " + tuple.getT2());
                System.out.println("  Op3: " + tuple.getT3());
            })
            .block();

        verifyConns(expected);

        pool.dispose();
        Thread.sleep(3000);
        System.out.println("Pool disposed for topology keys: " + topologyKeys);
    }

    private static Mono<String> runSlowQuery(ConnectionPool pool, int opId) {
        return Mono.from(pool.create())
            .flatMap(connection -> {
                System.out.println("Op" + opId + ": acquired connection");
                return Mono.from(connection.createStatement("SELECT pg_sleep(5), " + opId + " AS op_id").execute())
                    .flatMap(result -> Mono.from(result.map((row, meta) -> "op_id=" + row.get("op_id", Integer.class))))
                    .doFinally(sig -> {
                        System.out.println("Op" + opId + ": releasing connection");
                        Mono.from(connection.close()).subscribe();
                    });
            });
    }

    /**
     * Nodes 1,2 -> aws.us-west.us-west-2a
     * Nodes 3,4 -> aws.us-west.us-west-2b
     * Nodes 5,6 -> aws.us-west.us-west-2c
     */
    protected static void startYBDBClusterWithSixNodes() {
        executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
        executeCmd(path + "/bin/yb-ctl create --rf 3 --placement_info \"aws.us-west.us-west-2a,aws.us-west.us-west-2a,aws.us-west.us-west-2b\"",
            "Start YugabyteDB rf=3 cluster", 15);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2b\"",
            "Add a node", 10);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2c\"",
            "Add a node", 10);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2c\"",
            "Add a node", 10);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ie) {}
    }
}
