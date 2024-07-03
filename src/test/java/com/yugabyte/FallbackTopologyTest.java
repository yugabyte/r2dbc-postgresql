package com.yugabyte;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.UniformLoadBalancerConnectionStrategy;
import io.r2dbc.postgresql.api.PostgresqlConnection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FallbackTopologyTest extends UniformLoadbalancerTest {

    private static final String path = System.getenv("YBDB_PATH");
    private static final int numConnections = 12;

    public static void main (String[] args) throws InterruptedException {
        System.out.println("Checking Basic Behaviour...");
        // Start RF=3 cluster with placements 127.0.0.1 -> 2a, 127.0.0.2 -> 2b and 127.0.0.3 -> 2c
        startYBDBCluster();

        try {
            controlConnection = "127.0.0.3";
            // All valid/available placement zones
            createConnectionsAndVerify("aws.us-west.us-west-2a,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", Arrays.asList(6, 6, 0));
            createConnectionsAndVerify("aws.us-west.us-west-2a,aws.us-west.us-west-2c", Arrays.asList(6, 0, 6));
            createConnectionsAndVerify("aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws" + ".us-west.us-west-2c:3", Arrays.asList(12, 0, 0));
            createConnectionsAndVerify("aws.us-west.*,aws.us-west.us-west-2b:1,aws.us-west" + ".us-west-2c:2", Arrays.asList(4, 4, 4));
            createConnectionsAndVerify("aws.us-west.*:1,aws.us-west.us-west-2b:2,aws.us-west" + ".us-west-2c:3", Arrays.asList(4, 4, 4));

//            // Some invalid/unavailable placement zones
            createConnectionsAndVerify("BAD.BAD.BAD:1,aws.us-west.us-west-2b:2,aws.us-west" + ".us-west-2c:3", Arrays.asList(0, 12, 0));
            createConnectionsAndVerify("BAD.BAD.BAD:1,aws.us-west.us-west-2b:2,aws.us-west" + ".us-west-2c:2", Arrays.asList(0, 6, 6));
            createConnectionsAndVerify("aws.us-west.us-west-2a:1,BAD.BAD.BAD:2,aws.us-west" + ".us-west-2c:3", Arrays.asList(12, 0, 0));
            createConnectionsAndVerify("BAD.BAD.BAD:1,BAD.BAD.BAD:2,aws.us-west.us-west-2c:3", Arrays.asList(0, 0, 12));
            createConnectionsAndVerify("BAD.BAD.BAD:1,BAD.BAD.BAD:2,aws.us-west.*:3", Arrays.asList(4, 4, 4));

//            // Invalid preference value results in failure, value -1 indicates an error is expected.
            createConnectionsAndVerify("aws.us-west.us-west-2a:11,aws.us-west.us-west-2b:2,aws" + ".us-west.us-west-2c:3", Arrays.asList(-1, 0, 0));
            createConnectionsAndVerify("aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:-2,aws" + ".us-west.us-west-2c:3", Arrays.asList(-1, 0, 0));
            createConnectionsAndVerify("aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws" + ".us-west.us-west-2c:", Arrays.asList(-1, 0, 0));
        } finally {
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
            System.out.println("Done");
        }
    }


    protected static void createConnectionsAndVerify(String tkValues, List<Integer> expectedInput) {
        PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .addHost("127.0.0.1")
                .username("yugabyte")
                .password("yugabyte")
                .database("yugabyte")
                .loadBalanceHosts(true)
                .ybServersRefreshInterval(1)
                .topologyKeys(tkValues)
                .build());

        ArrayList<PostgresqlConnection> connections = new ArrayList<>();

        try {
            for (int i = 0; i < numConnections; i++) {
                PostgresqlConnection connection = connectionFactory.create().block();
                connections.add(connection);
            }
        }catch (Exception e){
            if (expectedInput.get(0) != -1){
                throw e;
            }
            System.out.println(e);
            if (!(e instanceof IllegalArgumentException)) {
                throw new RuntimeException("Did not expect this exception! ", e);
            }
            return;
        }
        System.out.println("Created " + numConnections + " connections");
        verifyConns(expectedInput);

        for (PostgresqlConnection connection: connections) {
            connection.close().block();
        }

    }

    protected static void startYBDBCluster() {
        executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
        executeCmd(path + "/bin/yb-ctl start --rf 3 --placement_info \"aws.us-west.us-west-2a,aws" +
                ".us-west.us-west-2b,aws.us-west.us-west-2c\"", "Start YugabyteDB rf=3 cluster", 15);
    }

    /**
     * Start RF=3 cluster with 6 nodes and with placements (127.0.0.1, 127.0.0.2, 127.0.0.3) -> us-west-1a,
     * and 127.0.0.4 -> us-west-2a, 127.0.0.5 -> us-west-2b and 127.0.0.6 -> us-west-2c
     */
    protected static void startYBDBClusterWithSixNodes() {
        executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);

        executeCmd(path + "/bin/yb-ctl --rf 3 start --placement_info \"aws.us-west.us-west-1a\" " +
                        "--tserver_flags \"placement_uuid=live,max_stale_read_bound_time_ms=60000000\"",
                "Start YugabyteDB rf=3 cluster", 15);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2a\"",
                "Add a node", 10);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2b\"",
                "Add a node", 10);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2c\"",
                "Add a node", 10);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ie) {}
    }
}
