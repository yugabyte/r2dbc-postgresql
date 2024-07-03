package com.yugabyte;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FallBackTopologyTestExtended extends FallbackTopologyTest{
    private static final String path = System.getenv("YBDB_PATH");

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Checking Multi Node Up Behaviour Without Closing Connections...");

        int numConnections = 12;

        startYBDBClusterWithSixNodes();

        try{

            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", Arrays.asList(6, 6, 0, 0, 0, 0));

            // Stop the 2 nodes in the primary zone

            executeCmd(path + "/bin/yb-ctl stop_node 1", "Stop node 1", 10);
            executeCmd(path + "/bin/yb-ctl stop_node 2", "Stop node 2", 10);
            Thread.sleep(10000);

            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", Arrays.asList(-1, -1, 6, 6, 0, 0));

            // Stop 1  node in the secondary zone

            executeCmd(path + "/bin/yb-ctl stop_node 3", "Stop node 3", 10);
            executeCmd(path + "/bin/yb-ctl stop_node 4", "Stop node 4", 10);
            Thread.sleep(10000);

            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", Arrays.asList(-1, -1, -1, -1, 6, 6));

            // Restart node 2

            executeCmd(path + "/bin/yb-ctl start_node 2 --placement_info \"aws.us-west.us-west-2a\" ", "Start node 2", 10);
            Thread.sleep(10000);
            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", Arrays.asList(-1, 12, -1, -1, 6, 6));

            executeCmd(path + "/bin/yb-ctl stop_node 2", "Stop node 2", 10);
            Thread.sleep(10000);
            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", Arrays.asList(-1, -1, -1, -1, 12, 12));

            executeCmd(path + "/bin/yb-ctl start_node 4 --placement_info \"aws.us-west.us-west-2b\" ", "Start node 4", 10);
            Thread.sleep(10000);
            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", Arrays.asList(-1, -1, -1, 12, 12, 12));

        }finally {
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
            System.out.println("Done");
        }
    }


    protected static void createConnectionsWithoutClosingAndVerify(int numConnections, String tkValues, List<Integer> expectedInput) throws InterruptedException {
        PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .addHost("127.0.0.1")
                .username("yugabyte")
                .password("yugabyte")
                .database("yugabyte")
                .loadBalanceHosts(true)
                .ybServersRefreshInterval(10)
                .topologyKeys(tkValues)
                .build());

        ArrayList<PostgresqlConnection> connections = new ArrayList<>();

        try {
            for (int i = 0; i < numConnections; i++) {
                if (i==9){
                    Thread.sleep(1000);
                }
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
    }

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
